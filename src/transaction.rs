use crate::btree::BTreeNode;
use crate::cache::PageCache;
use crate::compression::decompress;
use crate::error::{Result, SikioError};
use crate::page::{validate_key_value, OverflowPage, Page};
use crate::storage::OPFSStorage;
use crate::wal::WalEntry;

const VAL_TYPE_RAW: u8 = 0x00;
const OVERFLOW_MARKER_PREFIX: u8 = 0xFF;
const OVERFLOW_MARKER_SIZE: usize = 13;

pub enum TransactionOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct WriteTransaction {
    ops: Vec<TransactionOp>,
    wal_buffer: Vec<u8>,
    sequence_start: u64,
    committed: bool,
    aborted: bool,
}

impl WriteTransaction {
    pub fn new(current_sequence: u64) -> Self {
        WriteTransaction {
            ops: Vec::new(),
            wal_buffer: Vec::with_capacity(4096),
            sequence_start: current_sequence,
            committed: false,
            aborted: false,
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.committed || self.aborted {
            return Err(SikioError::Corrupted("Transaction not active".into()));
        }
        validate_key_value(&key, &value)?;

        let wrapped = wrap_raw_value(&value);
        self.ops.push(TransactionOp::Put {
            key,
            value: wrapped,
        });
        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        if self.committed || self.aborted {
            return Err(SikioError::Corrupted("Transaction not active".into()));
        }
        self.ops.push(TransactionOp::Delete { key });
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        !self.committed && !self.aborted
    }

    pub fn ops_count(&self) -> usize {
        self.ops.len()
    }

    pub fn sequence_start(&self) -> u64 {
        self.sequence_start
    }

    pub(crate) fn prepare_wal(&mut self, mut sequence: u64) -> u64 {
        self.wal_buffer.clear();

        for op in &self.ops {
            sequence += 1;
            match op {
                TransactionOp::Put { key, value } => {
                    WalEntry::serialize_put(sequence, key, value, &mut self.wal_buffer);
                }
                TransactionOp::Delete { key } => {
                    WalEntry::serialize_delete(sequence, key, &mut self.wal_buffer);
                }
            }
        }

        sequence += 1;
        WalEntry::serialize_commit(sequence, &mut self.wal_buffer);

        sequence
    }

    pub(crate) fn wal_bytes(&self) -> &[u8] {
        &self.wal_buffer
    }

    pub(crate) fn take_ops(&mut self) -> Vec<TransactionOp> {
        self.committed = true;
        std::mem::take(&mut self.ops)
    }

    pub fn abort(&mut self) {
        self.ops.clear();
        self.wal_buffer.clear();
        self.aborted = true;
    }
}

impl Drop for WriteTransaction {
    fn drop(&mut self) {
        if self.is_active() && !self.ops.is_empty() {
            self.ops.clear();
        }
    }
}

pub struct ReadTransaction {
    root_page_id: u64,
    created_at: u64,
}

impl ReadTransaction {
    pub fn new(root_page_id: u64) -> Self {
        ReadTransaction {
            root_page_id,
            created_at: js_sys::Date::now() as u64,
        }
    }

    pub fn root_page_id(&self) -> u64 {
        self.root_page_id
    }

    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    pub fn get(
        &self,
        key: &[u8],
        storage: &OPFSStorage,
        cache: &mut PageCache,
    ) -> Result<Option<Vec<u8>>> {
        if self.root_page_id == 0 {
            return Ok(None);
        }

        let mut current_page_id = self.root_page_id;

        loop {
            let node = load_node(current_page_id, storage, cache)?;
            let pos = node.find_key_position(key);

            if node.is_leaf {
                if pos < node.keys.len() && node.keys[pos] == key {
                    let stored_value = &node.values[pos];
                    return process_stored_value(stored_value, storage);
                }
                return Ok(None);
            } else if pos < node.children.len() {
                current_page_id = node.children[pos];
            } else {
                return Ok(None);
            }
        }
    }

    pub fn scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
        storage: &OPFSStorage,
        cache: &mut PageCache,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use crate::cursor::{cursor_next, cursor_seek, CursorState};

        let mut results = Vec::new();
        let mut state = CursorState::new();

        cursor_seek(&mut state, start_key, self.root_page_id, storage, cache)?;

        while state.valid() && results.len() < limit {
            if let (Some(key), Some(value)) = (state.key(), state.value()) {
                if key > end_key {
                    break;
                }
                if let Some(processed) = process_stored_value(value, storage)? {
                    results.push((key.to_vec(), processed));
                }
            }
            cursor_next(&mut state, storage, cache)?;
        }

        Ok(results)
    }
}

fn wrap_raw_value(value: &[u8]) -> Vec<u8> {
    let mut wrapped = Vec::with_capacity(1 + value.len());
    wrapped.push(VAL_TYPE_RAW);
    wrapped.extend_from_slice(value);
    wrapped
}

fn load_node(page_id: u64, storage: &OPFSStorage, cache: &mut PageCache) -> Result<BTreeNode> {
    if let Some(page) = cache.get(page_id) {
        return BTreeNode::from_page(page);
    }
    let bytes = storage.read_page(page_id)?;
    let page = Page::from_bytes(&bytes)?;
    let node = BTreeNode::from_page(&page)?;
    cache.insert(page, false);
    Ok(node)
}

fn process_stored_value(stored_value: &[u8], storage: &OPFSStorage) -> Result<Option<Vec<u8>>> {
    if stored_value.is_empty() {
        return Ok(None);
    }

    let first_byte = stored_value[0];

    if first_byte == OVERFLOW_MARKER_PREFIX && stored_value.len() >= OVERFLOW_MARKER_SIZE {
        let first_page_id = u64::from_le_bytes(
            stored_value[1..9]
                .try_into()
                .map_err(|_| SikioError::Corrupted("Invalid overflow marker".into()))?,
        );
        let total_len = u32::from_le_bytes(
            stored_value[9..13]
                .try_into()
                .map_err(|_| SikioError::Corrupted("Invalid overflow length".into()))?,
        ) as usize;

        let data = read_overflow_chain(first_page_id, total_len, storage)?;
        let decompressed = decompress(&data)
            .ok_or_else(|| SikioError::Corrupted("Failed to decompress overflow data".into()))?;
        return extract_value(&decompressed);
    }

    extract_value(stored_value)
}

fn extract_value(data: &[u8]) -> Result<Option<Vec<u8>>> {
    if data.is_empty() {
        return Ok(None);
    }

    let val_type = data[0];
    const VAL_TYPE_TTL: u8 = 0x01;

    match val_type {
        VAL_TYPE_RAW => Ok(Some(data[1..].to_vec())),
        VAL_TYPE_TTL => {
            if data.len() < 9 {
                return Ok(None);
            }
            let expiry = u64::from_le_bytes(
                data[1..9]
                    .try_into()
                    .map_err(|_| SikioError::Corrupted("Invalid TTL expiry".into()))?,
            );
            let now = js_sys::Date::now() as u64;
            if now > expiry {
                return Ok(None);
            }
            Ok(Some(data[9..].to_vec()))
        }
        _ => Ok(Some(data[1..].to_vec())),
    }
}

fn read_overflow_chain(
    first_page_id: u64,
    total_len: usize,
    storage: &OPFSStorage,
) -> Result<Vec<u8>> {
    let mut data = Vec::with_capacity(total_len);
    let mut current_page = first_page_id;

    while current_page != 0 && data.len() < total_len {
        let bytes = storage.read_page(current_page)?;
        let overflow = OverflowPage::from_bytes(&bytes)?;
        data.extend_from_slice(&overflow.data);
        current_page = overflow.next_page;
    }

    Ok(data)
}
