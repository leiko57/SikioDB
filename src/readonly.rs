use crate::btree::BTreeNode;
use crate::cache::PageCache;
use crate::compression::decompress;
use crate::cursor::{cursor_next, cursor_seek, CursorState};
use crate::error::{Result, SikioError};
use crate::page::Page;
use crate::storage::OPFSStorage;
use crate::page::{OverflowPage, OVERFLOW_DATA_SIZE};

const VAL_TYPE_RAW: u8 = 0x00;
const VAL_TYPE_TTL: u8 = 0x01;
const OVERFLOW_MARKER_PREFIX: u8 = 0xFF;
const OVERFLOW_MARKER_SIZE: usize = 13;
pub struct ReadOnlyDatabase {
    storage: OPFSStorage,
    cache: PageCache,
    root_page_id: u64,
}
impl ReadOnlyDatabase {
    pub fn open(storage: OPFSStorage, root_page_id: u64) -> Self {
        ReadOnlyDatabase {
            storage,
            cache: PageCache::with_capacity(128),
            root_page_id,
        }
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.root_page_id == 0 {
            return Ok(None);
        }
        let mut current_page_id = self.root_page_id;
        loop {
            let node = self.load_node(current_page_id)?;
            let pos = node.find_key_position(key);
            if node.is_leaf {
                if pos < node.keys.len() && node.keys[pos] == key {
                    let stored_value = &node.values[pos];
                    return self.process_value(stored_value);
                }
                return Ok(None);
            } else {
                if pos < node.children.len() {
                    current_page_id = node.children[pos];
                } else {
                    return Ok(None);
                }
            }
        }
    }
    pub fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let mut state = CursorState::new();
        cursor_seek(
            &mut state,
            start,
            self.root_page_id,
            &self.storage,
            &mut self.cache,
        )?;
        while state.valid() && results.len() < limit {
            if let (Some(key), Some(value)) = (state.key(), state.value()) {
                if key > end {
                    break;
                }
                if let Some(processed) = self.process_value(value)? {
                    results.push((key.to_vec(), processed));
                }
            }
            cursor_next(&mut state, &self.storage, &mut self.cache)?;
        }
        Ok(results)
    }
    pub fn exists(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key)?.is_some())
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
    fn process_value(&self, stored_value: &[u8]) -> Result<Option<Vec<u8>>> {
        if stored_value.is_empty() {
            return Ok(None);
        }

        if stored_value[0] == OVERFLOW_MARKER_PREFIX && stored_value.len() >= OVERFLOW_MARKER_SIZE {
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

            let data = read_overflow_chain(first_page_id, total_len, &self.storage)?;
            let wrapped = decompress(&data)
                .ok_or_else(|| SikioError::Corrupted("Failed to decompress overflow data".into()))?;
            return self.process_value(&wrapped);
        }

        match stored_value[0] {
            VAL_TYPE_RAW => Ok(Some(stored_value[1..].to_vec())),
            VAL_TYPE_TTL => {
                if stored_value.len() < 9 {
                    return Ok(None);
                }
                let expiry = u64::from_le_bytes(
                    stored_value[1..9]
                        .try_into()
                        .map_err(|_| SikioError::Corrupted("Invalid TTL expiry".into()))?,
                );
                let now = js_sys::Date::now() as u64;
                if now > expiry {
                    Ok(None)
                } else {
                    Ok(Some(stored_value[9..].to_vec()))
                }
            }
            _ => Ok(Some(stored_value[1..].to_vec())),
        }
    }
}

fn read_overflow_chain(first_page_id: u64, total_len: usize, storage: &OPFSStorage) -> Result<Vec<u8>> {
    if total_len == 0 {
        return Ok(Vec::new());
    }

    let mut data = Vec::with_capacity(total_len.min(OVERFLOW_DATA_SIZE));
    let mut current_page = first_page_id;
    while current_page != 0 && data.len() < total_len {
        let bytes = storage.read_page(current_page)?;
        let overflow = OverflowPage::from_bytes(&bytes)?;
        data.extend_from_slice(&overflow.data);
        current_page = overflow.next_page;
    }
    data.truncate(total_len);
    Ok(data)
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccessMode {
    ReadWrite,
    ReadOnly,
}
impl Default for AccessMode {
    fn default() -> Self {
        AccessMode::ReadWrite
    }
}
pub struct AccessGuard {
    mode: AccessMode,
}
impl AccessGuard {
    pub fn new(mode: AccessMode) -> Self {
        AccessGuard { mode }
    }
    pub fn check_write(&self) -> Result<()> {
        match self.mode {
            AccessMode::ReadWrite => Ok(()),
            AccessMode::ReadOnly => Err(SikioError::Corrupted(
                "Write operation not allowed in read-only mode".into(),
            )),
        }
    }
    pub fn is_read_only(&self) -> bool {
        self.mode == AccessMode::ReadOnly
    }
}
