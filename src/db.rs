use crate::btree::{BTree, BTreeNode};
use crate::cache::PageCache;
use crate::compression::{compress, decompress};
use crate::error::{Result, SikioError};
use crate::page::{validate_key_value, OverflowPage, Page, OVERFLOW_THRESHOLD, PAGE_SIZE};
use crate::range::{key_in_range, prefix_to_range, RangeBound};
use crate::storage::OPFSStorage;
use crate::transaction::{ReadTransaction, TransactionOp, WriteTransaction};
use crate::wal::{WalEntry, WalOperation, WalReader};
use wasm_bindgen::prelude::*;
const WAL_CHECKPOINT_THRESHOLD: u64 = 50 * 1024 * 1024;
const METADATA_PAGE_ID_1: u64 = 0;
const METADATA_PAGE_ID_2: u64 = 1;
const METADATA_MAGIC: u64 = 0x53494B494F4442;
const VAL_TYPE_RAW: u8 = 0x00;
const VAL_TYPE_TTL: u8 = 0x01;
const OVERFLOW_MARKER_PREFIX: u8 = 0xFF;
const OVERFLOW_MARKER_SIZE: usize = 13;
const BATCH_PAIRS_INITIAL_CAPACITY: usize = 1000;
fn wrap_raw_value(value: &[u8]) -> Vec<u8> {
    let mut wrapped = Vec::with_capacity(1 + value.len());
    wrapped.push(VAL_TYPE_RAW);
    wrapped.extend_from_slice(value);
    wrapped
}
fn wrap_ttl_value(value: &[u8], expiry: u64) -> Vec<u8> {
    let mut wrapped = Vec::with_capacity(1 + 8 + value.len());
    wrapped.push(VAL_TYPE_TTL);
    wrapped.extend_from_slice(&expiry.to_le_bytes());
    wrapped.extend_from_slice(value);
    wrapped
}
#[wasm_bindgen]
pub struct SikioDB {
    storage: OPFSStorage,
    btree: BTree,
    cache: PageCache,
    wal_sequence: u64,
}
impl Drop for SikioDB {
    fn drop(&mut self) {
        if let Err(_e) = self.flush() {}
    }
}
struct Metadata {
    root_page_id: u64,
    next_page_id: u64,
    wal_sequence: u64,
    free_page_ids: Vec<u64>,
}
const METADATA_HEADER_SIZE: usize = 40;
impl Metadata {
    fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0..8].copy_from_slice(&METADATA_MAGIC.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.root_page_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.next_page_id.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.wal_sequence.to_le_bytes());
        let free_count = self.free_page_ids.len() as u32;
        bytes[36..40].copy_from_slice(&free_count.to_le_bytes());
        let max_ids = (PAGE_SIZE - METADATA_HEADER_SIZE) / 8;
        let ids_to_write = self.free_page_ids.len().min(max_ids);
        for (i, &id) in self.free_page_ids.iter().take(ids_to_write).enumerate() {
            let offset = METADATA_HEADER_SIZE + i * 8;
            bytes[offset..offset + 8].copy_from_slice(&id.to_le_bytes());
        }
        let checksum = crc32fast::hash(&bytes[0..36]);
        bytes[32..36].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < METADATA_HEADER_SIZE {
            return None;
        }
        let magic = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        if magic != METADATA_MAGIC {
            return None;
        }
        let stored_checksum = u32::from_le_bytes(bytes[32..36].try_into().ok()?);
        let mut check_bytes = bytes[0..36].to_vec();
        check_bytes[32..36].copy_from_slice(&[0u8; 4]);
        let computed = crc32fast::hash(&check_bytes[0..36]);
        if stored_checksum != computed {
            return None;
        }
        let root_page_id = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let next_page_id = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let wal_sequence = u64::from_le_bytes(bytes[24..32].try_into().ok()?);
        let free_count = u32::from_le_bytes(bytes[36..40].try_into().ok()?) as usize;
        let max_ids = (PAGE_SIZE - METADATA_HEADER_SIZE) / 8;
        let ids_to_read = free_count.min(max_ids);
        let mut free_page_ids = Vec::with_capacity(ids_to_read);
        for i in 0..ids_to_read {
            let offset = METADATA_HEADER_SIZE + i * 8;
            if offset + 8 <= bytes.len() {
                let id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
                free_page_ids.push(id);
            }
        }
        Some(Metadata {
            root_page_id,
            next_page_id,
            wal_sequence,
            free_page_ids,
        })
    }
}
#[wasm_bindgen]
impl SikioDB {
    #[wasm_bindgen(js_name = open)]
    pub async fn js_open(db_name: &str) -> std::result::Result<SikioDB, JsValue> {
        Self::open_internal(db_name)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    async fn open_internal(db_name: &str) -> Result<SikioDB> {
        let storage = OPFSStorage::open(db_name)
            .await
            .map_err(|e| SikioError::IoError(format!("{:?}", e)))?;
        let mut db = SikioDB {
            storage,
            btree: BTree::new(),
            cache: PageCache::new(),
            wal_sequence: 0,
        };
        db.recover()?;
        Ok(db)
    }
    fn recover(&mut self) -> Result<()> {
        let mut best_metadata: Option<Metadata> = None;
        if self.storage.data_page_count() > 0 {
            if let Ok(bytes_0) = self.storage.read_page(METADATA_PAGE_ID_1) {
                if let Some(meta_0) = Metadata::from_bytes(&bytes_0) {
                    best_metadata = Some(meta_0);
                }
            }
        }
        if self.storage.data_page_count() > 1 {
            if let Ok(bytes_1) = self.storage.read_page(METADATA_PAGE_ID_2) {
                if let Some(meta_1) = Metadata::from_bytes(&bytes_1) {
                    match best_metadata {
                        Some(ref current) => {
                            if meta_1.wal_sequence > current.wal_sequence {
                                best_metadata = Some(meta_1);
                            }
                        }
                        None => {
                            best_metadata = Some(meta_1);
                        }
                    }
                }
            }
        }
        let mut has_valid_metadata = false;
        if let Some(meta) = best_metadata {
            if meta.root_page_id > 0 {
                self.btree = BTree::with_root(meta.root_page_id, meta.next_page_id);
                self.btree.set_free_page_ids(meta.free_page_ids);
                self.wal_sequence = meta.wal_sequence;
                has_valid_metadata = true;
            }
        }
        if !has_valid_metadata {
            self.initialize_empty_db()?;
        }
        if self.storage.wal_size() > 0 {
            self.replay_wal()?;
        }
        Ok(())
    }
    fn replay_wal(&mut self) -> Result<()> {
        let wal_size = self.storage.wal_size() as usize;
        if wal_size == 0 {
            return Ok(());
        }
        let wal_data = self.storage.read_wal(0, wal_size)?;
        let reader = WalReader::new(&wal_data);
        let mut committed_ops: Vec<WalEntry> = Vec::new();
        let mut pending_ops: Vec<WalEntry> = Vec::new();
        let mut _last_checkpoint_seq = 0u64;
        for entry_result in reader {
            match entry_result {
                Ok(entry) => match entry.operation {
                    WalOperation::Checkpoint => {
                        _last_checkpoint_seq = entry.sequence;
                        committed_ops.clear();
                        pending_ops.clear();
                    }
                    WalOperation::Commit => {
                        committed_ops.append(&mut pending_ops);
                    }
                    WalOperation::Put | WalOperation::Delete => {
                        pending_ops.push(entry);
                    }
                },
                Err(_) => break,
            }
        }
        for entry in committed_ops {
            match entry.operation {
                WalOperation::Put => {
                    if let Some(value) = entry.value {
                        self.apply_put(&entry.key, &value)?;
                    }
                }
                WalOperation::Delete => {
                    self.apply_delete(&entry.key)?;
                }
                _ => {}
            }
        }
        self.checkpoint()?;
        Ok(())
    }
    fn initialize_empty_db(&mut self) -> Result<()> {
        let root_id = self.btree.allocate_page();
        let root = BTreeNode::new_leaf(root_id);
        let page = root.to_page()?;
        self.storage.write_page(root_id, &page.to_bytes())?;
        self.btree.set_root(root_id);
        let meta = Metadata {
            root_page_id: self.btree.root_page_id(),
            next_page_id: self.btree.next_page_id(),
            wal_sequence: self.wal_sequence,
            free_page_ids: self.btree.free_page_ids().to_vec(),
        };
        let bytes = meta.to_bytes();
        self.storage.write_page(METADATA_PAGE_ID_1, &bytes)?;
        self.storage.write_page(METADATA_PAGE_ID_2, &bytes)?;
        Ok(())
    }
    fn write_metadata(&mut self) -> Result<()> {
        let meta = Metadata {
            root_page_id: self.btree.root_page_id(),
            next_page_id: self.btree.next_page_id(),
            wal_sequence: self.wal_sequence,
            free_page_ids: self.btree.free_page_ids().to_vec(),
        };
        let bytes = meta.to_bytes();
        let target_page = if self.wal_sequence % 2 == 0 {
            METADATA_PAGE_ID_1
        } else {
            METADATA_PAGE_ID_2
        };
        self.storage.write_page(target_page, &bytes)?;
        Ok(())
    }
    #[wasm_bindgen]
    pub fn verify_integrity(&mut self) -> std::result::Result<Box<[u64]>, JsValue> {
        let count = self.storage.data_page_count();
        let mut corrupted_pages = Vec::new();
        for page_id in 2..count {
            match self.storage.read_page(page_id) {
                Ok(bytes) => {
                    if let Err(_) = Page::from_bytes(&bytes) {
                        corrupted_pages.push(page_id);
                    }
                }
                Err(_) => {
                    corrupted_pages.push(page_id);
                }
            }
        }
        Ok(corrupted_pages.into_boxed_slice())
    }
    #[wasm_bindgen]
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> std::result::Result<(), JsValue> {
        let wrapped = wrap_raw_value(value);
        self.put_internal(key, &wrapped)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    #[wasm_bindgen(js_name = putWithTTL)]
    pub fn put_with_ttl(
        &mut self,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
    ) -> std::result::Result<(), JsValue> {
        let now = js_sys::Date::now() as u64;
        let expiry = now + ttl_ms;
        let wrapped = wrap_ttl_value(value, expiry);
        self.put_internal(key, &wrapped)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    #[wasm_bindgen]
    pub fn put_batch(&mut self, data: &[u8]) -> std::result::Result<u32, JsValue> {
        self.put_batch_internal(data)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    #[wasm_bindgen(js_name = putNoSync)]
    pub fn put_no_sync(&mut self, key: &[u8], value: &[u8]) -> std::result::Result<(), JsValue> {
        let wrapped = wrap_raw_value(value);
        self.put_internal_no_sync(key, &wrapped)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    #[wasm_bindgen]
    pub fn flush(&mut self) -> std::result::Result<(), JsValue> {
        self.storage
            .flush_wal()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        self.storage
            .flush_data()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    }
    #[wasm_bindgen]
    pub fn close(&mut self) -> std::result::Result<(), JsValue> {
        self.flush()?;
        self.checkpoint()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    #[wasm_bindgen(js_name = scanRange)]
    pub fn scan_range(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        limit: u32,
    ) -> std::result::Result<js_sys::Array, JsValue> {
        use crate::cursor::{cursor_next, cursor_seek, CursorState};
        let mut state = CursorState::new();
        let root = self.btree.root_page_id();
        let results = js_sys::Array::new();
        let mut count = 0u32;

        cursor_seek(&mut state, start_key, root, &self.storage, &mut self.cache)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        while state.valid() && count < limit {
            if let (Some(key), Some(value)) = (state.key(), state.value()) {
                if &key[..] > end_key {
                    break;
                }

                let user_value = if value.is_empty() {
                    None
                } else {
                    match value[0] {
                        VAL_TYPE_RAW => Some(value[1..].to_vec()),
                        VAL_TYPE_TTL => {
                            if value.len() >= 9 {
                                let expiry =
                                    u64::from_le_bytes(value[1..9].try_into().unwrap_or([0; 8]));
                                let now = js_sys::Date::now() as u64;
                                if now <= expiry {
                                    Some(value[9..].to_vec())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        _ => None, // Unknown type or overflow marker (already resolved by cursor?)
                    }
                };


                if let Some(val) = user_value {
                    let entry = js_sys::Object::new();
                    let key_arr = js_sys::Uint8Array::from(&key[..]);
                    let val_arr = js_sys::Uint8Array::from(&val[..]);

                    js_sys::Reflect::set(&entry, &"key".into(), &key_arr)?;
                    js_sys::Reflect::set(&entry, &"value".into(), &val_arr)?;

                    results.push(&entry);
                    count += 1;
                }
            }
            cursor_next(&mut state, &self.storage, &mut self.cache)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
        }
        Ok(results)
    }
    fn put_batch_internal(&mut self, data: &[u8]) -> Result<u32> {
        let mut offset = 0;
        let mut count = 0;
        let len = data.len();
        let mut wal_buffer = Vec::with_capacity(len + (len / 10));
        let mut pairs = Vec::with_capacity(BATCH_PAIRS_INITIAL_CAPACITY);
        while offset < len {
            if offset + 4 > len {
                break;
            }
            let key_len = u32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| SikioError::Corrupted("Invalid key len in batch".into()))?,
            ) as usize;
            offset += 4;
            if offset + key_len > len {
                break;
            }
            let key = &data[offset..offset + key_len];
            offset += key_len;
            if offset + 4 > len {
                break;
            }
            let val_len = u32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| SikioError::Corrupted("Invalid val len in batch".into()))?,
            ) as usize;
            offset += 4;
            if offset + val_len > len {
                break;
            }
            let value = &data[offset..offset + val_len];
            offset += val_len;
            let mut wrapped = Vec::with_capacity(1 + value.len());
            wrapped.push(VAL_TYPE_RAW);
            wrapped.extend_from_slice(value);
            pairs.push((key.to_vec(), wrapped));
            count += 1;
        }
        for (key, value) in &pairs {
            validate_key_value(key, value)?;
            self.wal_sequence += 1;
            WalEntry::serialize_put(self.wal_sequence, key, value, &mut wal_buffer);
        }
        self.wal_sequence += 1;
        WalEntry::serialize_commit(self.wal_sequence, &mut wal_buffer);
        self.storage.append_wal(&wal_buffer)?;
        self.storage.flush_wal()?;
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        for (key, value) in &pairs {
            self.apply_put(key, value)?;
        }
        if self.storage.wal_size() > WAL_CHECKPOINT_THRESHOLD {
            self.checkpoint()?;
        }
        Ok(count)
    }
    fn put_internal(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        validate_key_value(key, value)?;
        self.wal_sequence += 1;
        let wal_entry = WalEntry::new_put(self.wal_sequence, key.to_vec(), value.to_vec());
        self.storage.append_wal(&wal_entry.to_bytes())?;
        self.wal_sequence += 1;
        let commit = WalEntry::new_commit(self.wal_sequence);
        self.storage.append_wal(&commit.to_bytes())?;
        self.storage.flush_wal()?;
        self.apply_put(key, value)?;
        if self.storage.wal_size() > WAL_CHECKPOINT_THRESHOLD {
            self.checkpoint()?;
        }
        Ok(())
    }
    fn put_internal_no_sync(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        validate_key_value(key, value)?;
        self.wal_sequence += 1;
        let wal_entry = WalEntry::new_put(self.wal_sequence, key.to_vec(), value.to_vec());
        self.storage.append_wal(&wal_entry.to_bytes())?;
        self.wal_sequence += 1;
        let commit = WalEntry::new_commit(self.wal_sequence);
        self.storage.append_wal(&commit.to_bytes())?;
        self.apply_put(key, value)?;
        if self.storage.wal_size() > WAL_CHECKPOINT_THRESHOLD {
            self.checkpoint()?;
        }
        Ok(())
    }
    fn apply_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if value.len() > OVERFLOW_THRESHOLD {
            let compressed = compress(value);
            let (start_page, total_len) = self.write_overflow_chain(&compressed)?;
            let marker = Self::encode_overflow_marker(start_page, total_len);
            self.apply_put_value(key, &marker)
        } else {
            self.apply_put_value(key, value)
        }
    }
    fn apply_put_value(&mut self, key: &[u8], stored_value: &[u8]) -> Result<()> {
        let root_id = self.btree.root_page_id();
        let result = self.insert_recursive(root_id, key, stored_value)?;
        if let Some((separator, new_right_id)) = result {
            let new_root_id = self.btree.allocate_page();
            let mut new_root = BTreeNode::new_internal(new_root_id);
            new_root.keys.push(separator);
            new_root.children.push(root_id);
            new_root.children.push(new_right_id);
            let page = new_root.to_page()?;
            self.cache.insert(page, true);
            self.btree.set_root(new_root_id);
        }
        Ok(())
    }
    fn insert_recursive(
        &mut self,
        page_id: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, u64)>> {
        let mut node = self.load_node(page_id)?;
        let pos = node.find_key_position(key);
        if node.is_leaf {
            if pos < node.keys.len() && node.keys[pos] == key {
                node.values[pos] = value.to_vec();
            } else {
                node.insert_at(pos, key.to_vec(), value.to_vec());
            }
            if node.needs_split() {
                let (separator, mut right) = node.split();
                let right_id = self.btree.allocate_page();
                right.page_id = right_id;
                self.save_node(&node)?;
                self.save_node(&right)?;
                return Ok(Some((separator, right_id)));
            }
            self.save_node(&node)?;
            Ok(None)
        } else {
            let child_idx = if pos < node.keys.len() && node.keys[pos] == key {
                pos + 1
            } else {
                pos
            };
            if child_idx >= node.children.len() {
                return Err(SikioError::PageCorrupted {
                    page_id: node.page_id,
                    reason: "Internal node missing children".into(),
                });
            }
            let child_id = node.children[child_idx];
            let child_result = self.insert_recursive(child_id, key, value)?;
            if let Some((separator, new_child_id)) = child_result {
                node.insert_internal(child_idx, separator, new_child_id);
                if node.needs_split() {
                    let (sep, mut right) = node.split();
                    let right_id = self.btree.allocate_page();
                    right.page_id = right_id;
                    self.save_node(&node)?;
                    self.save_node(&right)?;
                    return Ok(Some((sep, right_id)));
                }
                self.save_node(&node)?;
            }
            Ok(None)
        }
    }
    #[wasm_bindgen]
    pub fn get(&mut self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, JsValue> {
        self.get_internal(key)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    fn get_internal(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let root_id = self.btree.root_page_id();
        let stored_value = self.search_recursive(root_id, key)?;
        let raw_data = match stored_value {
            Some(data) if Self::is_overflow_marker(&data) => {
                let (start_page, total_len) =
                    Self::decode_overflow_marker(&data).ok_or_else(|| {
                        SikioError::PageCorrupted {
                            page_id: 0,
                            reason: "Invalid overflow marker".into(),
                        }
                    })?;
                let compressed = self.read_overflow_chain(start_page, total_len)?;
                decompress(&compressed).ok_or_else(|| {
                    SikioError::Corrupted("Failed to decompress overflow data".into())
                })?
            }
            Some(data) => data,
            None => return Ok(None),
        };

        if raw_data.is_empty() {
            return Ok(None);
        }
        match raw_data[0] {
            VAL_TYPE_RAW => Ok(Some(raw_data[1..].to_vec())),
            VAL_TYPE_TTL => {
                if raw_data.len() < 9 {
                    return Ok(None);
                }
                let expiry = u64::from_le_bytes(
                    raw_data[1..9]
                        .try_into()
                        .map_err(|_| SikioError::Corrupted("Invalid TTL expiry".into()))?,
                );
                let now = js_sys::Date::now() as u64;
                if now > expiry {
                    Ok(None)
                } else {
                    Ok(Some(raw_data[9..].to_vec()))
                }
            }
            _ => Err(SikioError::PageCorrupted {
                page_id: 0,
                reason: format!("Unknown Value Type: {}", raw_data[0]),
            }),
        }
    }
    fn search_recursive(&mut self, page_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let node = self.load_node(page_id)?;
        let pos = node.find_key_position(key);
        if node.is_leaf {
            if pos < node.keys.len() && node.keys[pos] == key {
                Ok(Some(node.values[pos].clone()))
            } else {
                Ok(None)
            }
        } else {
            let child_idx = if pos < node.keys.len() && node.keys[pos] == key {
                pos + 1
            } else {
                pos
            };
            if child_idx >= node.children.len() {
                return Ok(None);
            }
            let child_id = node.children[child_idx];
            self.search_recursive(child_id, key)
        }
    }
    #[wasm_bindgen]
    pub fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> std::result::Result<Vec<js_sys::Uint8Array>, JsValue> {
        self.scan_prefix_internal(prefix)
            .map(|pairs| {
                pairs
                    .into_iter()
                    .flat_map(|(k, v)| {
                        vec![
                            js_sys::Uint8Array::from(k.as_slice()),
                            js_sys::Uint8Array::from(v.as_slice()),
                        ]
                    })
                    .collect()
            })
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    fn scan_prefix_internal(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (start, end) = prefix_to_range(prefix);
        let raw_pairs = self.range_internal(&start, &end)?;
        let mut clean_pairs = Vec::with_capacity(raw_pairs.len());
        let now = js_sys::Date::now() as u64;
        for (key, val) in raw_pairs {
            if val.is_empty() {
                continue;
            }
            match val[0] {
                VAL_TYPE_RAW => clean_pairs.push((key, val[1..].to_vec())),
                VAL_TYPE_TTL => {
                    if val.len() < 9 {
                        continue;
                    }
                    let expiry_bytes = match val[1..9].try_into() {
                        Ok(b) => b,
                        Err(_) => continue,
                    };
                    let expiry = u64::from_le_bytes(expiry_bytes);
                    if now <= expiry {
                        clean_pairs.push((key, val[9..].to_vec()));
                    }
                }
                _ => {}
            }
        }
        Ok(clean_pairs)
    }
    fn range_internal(
        &mut self,
        start: &RangeBound,
        end: &RangeBound,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let root_id = self.btree.root_page_id();
        let mut results = Vec::new();
        self.collect_range(root_id, start, end, &mut results)?;
        Ok(results)
    }
    fn collect_range(
        &mut self,
        page_id: u64,
        start: &RangeBound,
        end: &RangeBound,
        results: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let node = self.load_node(page_id)?;
        if node.is_leaf {
            for i in 0..node.keys.len() {
                if key_in_range(&node.keys[i], start, end) {
                    let value = self.get_value_resolved(&node.values[i])?;
                    results.push((node.keys[i].clone(), value));
                }
            }
        } else {
            for i in 0..node.children.len() {
                let should_descend = if i == 0 {
                    match start.start_key() {
                        None => true,
                        Some(sk) => i < node.keys.len() && node.keys[i].as_slice() >= sk,
                    }
                } else if i == node.children.len() - 1 {
                    true
                } else {
                    true
                };
                if should_descend {
                    self.collect_range(node.children[i], start, end, results)?;
                }
            }
        }
        Ok(())
    }
    fn get_value_resolved(&self, stored: &[u8]) -> Result<Vec<u8>> {
        if Self::is_overflow_marker(stored) {
            let (start_page, total_len) =
                Self::decode_overflow_marker(stored).ok_or_else(|| SikioError::PageCorrupted {
                    page_id: 0,
                    reason: "Invalid overflow marker".into(),
                })?;
            let compressed_data = self.read_overflow_chain(start_page, total_len)?;
            Ok(decompress(&compressed_data).unwrap_or(compressed_data))
        } else {
            Ok(stored.to_vec())
        }
    }
    #[wasm_bindgen]
    pub fn delete(&mut self, key: &[u8]) -> std::result::Result<bool, JsValue> {
        self.delete_internal(key)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    fn delete_internal(&mut self, key: &[u8]) -> Result<bool> {
        self.wal_sequence += 1;
        let wal_entry = WalEntry::new_delete(self.wal_sequence, key.to_vec());
        self.storage.append_wal(&wal_entry.to_bytes())?;
        self.wal_sequence += 1;
        let commit = WalEntry::new_commit(self.wal_sequence);
        self.storage.append_wal(&commit.to_bytes())?;
        self.storage.flush_wal()?;
        let deleted = self.apply_delete(key)?;
        if self.storage.wal_size() > WAL_CHECKPOINT_THRESHOLD {
            self.checkpoint()?;
        }
        Ok(deleted)
    }
    fn apply_delete(&mut self, key: &[u8]) -> Result<bool> {
        let root_id = self.btree.root_page_id();
        let (deleted, _) = self.delete_recursive_rebalance(root_id, key)?;
        if deleted {
            let root = self.load_node(root_id)?;
            if !root.is_leaf && root.keys.is_empty() && root.children.len() == 1 {
                self.btree.set_root(root.children[0]);
            }
        }
        Ok(deleted)
    }
    fn delete_recursive_rebalance(&mut self, page_id: u64, key: &[u8]) -> Result<(bool, bool)> {
        let mut node = self.load_node(page_id)?;
        let pos = node.find_key_position(key);
        if node.is_leaf {
            if pos < node.keys.len() && node.keys[pos] == key {
                node.keys.remove(pos);
                node.values.remove(pos);
                let underflow = node.is_underflow();
                self.save_node(&node)?;
                Ok((true, underflow))
            } else {
                Ok((false, false))
            }
        } else {
            let child_idx = if pos < node.keys.len() && node.keys[pos] == key {
                pos + 1
            } else {
                pos
            };
            if child_idx >= node.children.len() {
                return Ok((false, false));
            }
            let child_id = node.children[child_idx];
            let (deleted, child_underflow) = self.delete_recursive_rebalance(child_id, key)?;
            if !deleted {
                return Ok((false, false));
            }
            if child_underflow {
                self.rebalance_child(&mut node, child_idx)?;
            }
            let node_underflow = node.is_underflow();
            self.save_node(&node)?;
            Ok((true, node_underflow))
        }
    }
    fn rebalance_child(&mut self, parent: &mut BTreeNode, child_idx: usize) -> Result<()> {
        let child_id = parent.children[child_idx];
        let mut child = self.load_node(child_id)?;
        if child_idx > 0 {
            let left_sibling_id = parent.children[child_idx - 1];
            let mut left_sibling = self.load_node(left_sibling_id)?;
            if left_sibling.can_lend() {
                let parent_key = parent.keys[child_idx - 1].clone();
                let new_parent_key = child.borrow_from_left(&mut left_sibling, parent_key)?;
                parent.keys[child_idx - 1] = new_parent_key;
                self.save_node(&left_sibling)?;
                self.save_node(&child)?;
                return Ok(());
            }
        }
        if child_idx < parent.children.len() - 1 {
            let right_sibling_id = parent.children[child_idx + 1];
            let mut right_sibling = self.load_node(right_sibling_id)?;
            if right_sibling.can_lend() {
                let parent_key = parent.keys[child_idx].clone();
                let new_parent_key = child.borrow_from_right(&mut right_sibling, parent_key)?;
                parent.keys[child_idx] = new_parent_key;
                self.save_node(&right_sibling)?;
                self.save_node(&child)?;
                return Ok(());
            }
        }
        if child_idx > 0 {
            let left_sibling_id = parent.children[child_idx - 1];
            let left_sibling = self.load_node(left_sibling_id)?;
            let mut merged = left_sibling;
            let separator = parent.keys.remove(child_idx - 1);
            parent.children.remove(child_idx);
            merged.merge_with(child, separator);
            merged.page_id = left_sibling_id;
            self.save_node(&merged)?;
        } else if child_idx < parent.children.len() - 1 {
            let right_sibling_id = parent.children[child_idx + 1];
            let right_sibling = self.load_node(right_sibling_id)?;
            let separator = parent.keys.remove(child_idx);
            parent.children.remove(child_idx + 1);
            child.merge_with(right_sibling, separator);
            self.save_node(&child)?;
        }
        Ok(())
    }
    fn load_node(&mut self, page_id: u64) -> Result<BTreeNode> {
        if let Some(page) = self.cache.get(page_id) {
            return BTreeNode::from_page(page);
        }
        let bytes = self.storage.read_page(page_id)?;
        let page = Page::from_bytes(&bytes)?;
        let node = BTreeNode::from_page(&page)?;
        self.cache.insert(page, false);
        Ok(node)
    }
    fn save_node(&mut self, node: &BTreeNode) -> Result<()> {
        let page = node.to_page()?;
        self.cache.insert(page, true);
        Ok(())
    }
    fn write_overflow_chain(&mut self, data: &[u8]) -> Result<(u64, u32)> {
        if data.is_empty() {
            return Err(SikioError::IoError(
                "Cannot write empty overflow chain".into(),
            ));
        }
        let max_per_page = OverflowPage::max_data_per_page();
        let mut remaining = data;
        let mut pages: Vec<OverflowPage> = Vec::new();
        while !remaining.is_empty() {
            let chunk_size = remaining.len().min(max_per_page);
            let page_id = self.btree.allocate_page();
            let mut page = OverflowPage::new(page_id);
            page.data = remaining[..chunk_size].to_vec();
            page.data_length = chunk_size as u32;
            pages.push(page);
            remaining = &remaining[chunk_size..];
        }
        for i in 0..pages.len().saturating_sub(1) {
            pages[i].next_page = pages[i + 1].page_id;
        }
        for page in &pages {
            self.storage.write_page(page.page_id, &page.to_bytes())?;
        }
        let start_page = pages
            .first()
            .ok_or_else(|| SikioError::IoError("Empty overflow chain".into()))?
            .page_id;
        let total_len = data.len() as u32;
        Ok((start_page, total_len))
    }
    fn read_overflow_chain(&self, start_page: u64, total_len: u32) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(total_len as usize);
        let mut current_page_id = start_page;
        while current_page_id != 0 && result.len() < total_len as usize {
            let bytes = self.storage.read_page(current_page_id)?;
            let page = OverflowPage::from_bytes(&bytes)?;
            result.extend_from_slice(&page.data);
            current_page_id = page.next_page;
        }
        result.truncate(total_len as usize);
        Ok(result)
    }
    fn encode_overflow_marker(start_page: u64, total_len: u32) -> Vec<u8> {
        let mut marker = Vec::with_capacity(OVERFLOW_MARKER_SIZE);
        marker.push(OVERFLOW_MARKER_PREFIX);
        marker.extend_from_slice(&start_page.to_le_bytes());
        marker.extend_from_slice(&total_len.to_le_bytes());
        marker
    }
    fn decode_overflow_marker(data: &[u8]) -> Option<(u64, u32)> {
        if data.len() >= OVERFLOW_MARKER_SIZE && data[0] == OVERFLOW_MARKER_PREFIX {
            let start_page = u64::from_le_bytes(data[1..9].try_into().ok()?);
            let total_len = u32::from_le_bytes(data[9..OVERFLOW_MARKER_SIZE].try_into().ok()?);
            Some((start_page, total_len))
        } else {
            None
        }
    }
    fn is_overflow_marker(data: &[u8]) -> bool {
        data.len() >= OVERFLOW_MARKER_SIZE && data[0] == OVERFLOW_MARKER_PREFIX
    }
    fn checkpoint(&mut self) -> Result<()> {
        let dirty_ids = self.cache.dirty_pages();
        let mut sorted_ids = dirty_ids;
        sorted_ids.sort();
        for page_id in sorted_ids {
            if let Some(page) = self.cache.get(page_id) {
                let bytes = page.to_bytes();
                self.storage.write_page(page_id, &bytes)?;
            }
        }
        self.storage.flush_data()?;
        self.write_metadata()?;
        self.storage.flush_data()?;
        self.storage.truncate_wal()?;
        self.wal_sequence = 0;
        self.cache.clear_dirty();
        Ok(())
    }

    #[wasm_bindgen(js_name = beginWriteTxn)]
    pub fn js_begin_write(&mut self) -> JsWriteTransaction {
        JsWriteTransaction::new(self.wal_sequence)
    }

    #[wasm_bindgen(js_name = commitTxn)]
    pub fn js_commit_transaction(
        &mut self,
        txn: &mut JsWriteTransaction,
    ) -> std::result::Result<(), JsValue> {
        if !txn.is_active() {
            return Err(JsValue::from_str("Transaction already finished"));
        }

        if txn.ops_count() == 0 {
            txn.abort();
            return Ok(());
        }

        let new_sequence = txn.prepare_wal(self.wal_sequence);

        self.storage
            .append_wal(&txn.get_wal_bytes())
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        self.storage
            .flush_wal()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.wal_sequence = new_sequence;

        let ops = txn.take_ops_internal();
        for op in ops {
            match op {
                TransactionOp::Put { key, value } => {
                    self.apply_put(&key, &value)
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;
                }
                TransactionOp::Delete { key } => {
                    self.apply_delete(&key)
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;
                }
            }
        }

        if self.storage.wal_size() > WAL_CHECKPOINT_THRESHOLD {
            self.checkpoint()
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
        }

        Ok(())
    }
}

impl SikioDB {
    pub fn begin_write(&mut self) -> WriteTransaction {
        WriteTransaction::new(self.wal_sequence)
    }

    pub fn begin_read(&self) -> ReadTransaction {
        ReadTransaction::new(self.btree.root_page_id())
    }

    pub fn commit_transaction(&mut self, txn: &mut WriteTransaction) -> Result<()> {
        if !txn.is_active() {
            return Err(SikioError::Corrupted("Transaction already finished".into()));
        }

        if txn.ops_count() == 0 {
            txn.abort();
            return Ok(());
        }

        let new_sequence = txn.prepare_wal(self.wal_sequence);

        self.storage.append_wal(txn.wal_bytes())?;
        self.storage.flush_wal()?;

        self.wal_sequence = new_sequence;

        let ops = txn.take_ops();
        for op in ops {
            match op {
                TransactionOp::Put { key, value } => {
                    self.apply_put(&key, &value)?;
                }
                TransactionOp::Delete { key } => {
                    self.apply_delete(&key)?;
                }
            }
        }

        if self.storage.wal_size() > WAL_CHECKPOINT_THRESHOLD {
            self.checkpoint()?;
        }

        Ok(())
    }
}

#[wasm_bindgen]
pub struct JsWriteTransaction {
    inner: WriteTransaction,
}

#[wasm_bindgen]
impl JsWriteTransaction {
    #[wasm_bindgen(constructor)]
    pub fn new(sequence: u64) -> Self {
        JsWriteTransaction {
            inner: WriteTransaction::new(sequence),
        }
    }

    #[wasm_bindgen]
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> std::result::Result<(), JsValue> {
        self.inner
            .put(key.to_vec(), value.to_vec())
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen]
    pub fn delete(&mut self, key: &[u8]) -> std::result::Result<(), JsValue> {
        self.inner
            .delete(key.to_vec())
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen]
    pub fn abort(&mut self) {
        self.inner.abort();
    }

    #[wasm_bindgen(js_name = isActive)]
    pub fn is_active_js(&self) -> bool {
        self.inner.is_active()
    }
}

impl JsWriteTransaction {
    pub fn is_active(&self) -> bool {
        self.inner.is_active()
    }

    pub fn ops_count(&self) -> usize {
        self.inner.ops_count()
    }

    pub fn prepare_wal(&mut self, sequence: u64) -> u64 {
        self.inner.prepare_wal(sequence)
    }

    pub fn get_wal_bytes(&self) -> Vec<u8> {
        self.inner.wal_bytes().to_vec()
    }

    pub fn take_ops_internal(&mut self) -> Vec<TransactionOp> {
        self.inner.take_ops()
    }
}
