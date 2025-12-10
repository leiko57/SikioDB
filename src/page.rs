use crate::error::{Result, SikioError};
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 24;
pub const PAGE_DATA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;
pub const MAX_KEY_SIZE: usize = 1024;
pub const MAX_VALUE_SIZE: usize = 64 * 1024 * 1024;
pub const OVERFLOW_THRESHOLD: usize = 1024;
pub const MAX_KV_SIZE: usize = MAX_KEY_SIZE + OVERFLOW_THRESHOLD;
pub const PAGE_TYPE_INTERNAL: u8 = 1;
pub const PAGE_TYPE_LEAF: u8 = 2;
pub const PAGE_TYPE_OVERFLOW: u8 = 3;
pub const PAGE_TYPE_FREE: u8 = 0;
#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_id: u64,
    pub page_type: u8,
    pub item_count: u16,
    pub free_space_offset: u16,
    pub checksum: u32,
}
impl PageHeader {
    pub fn new(page_id: u64, page_type: u8) -> Self {
        PageHeader {
            page_id,
            page_type,
            item_count: 0,
            free_space_offset: PAGE_DATA_SIZE as u16,
            checksum: 0,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Page {
    pub header: PageHeader,
    pub data: Vec<u8>,
}
impl Page {
    pub fn new(page_id: u64, page_type: u8) -> Self {
        Page {
            header: PageHeader::new(page_id, page_type),
            data: vec![0u8; PAGE_DATA_SIZE],
        }
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(SikioError::PageCorrupted {
                page_id: 0,
                reason: format!("Invalid page size: {}", bytes.len()),
            });
        }
        let header_bytes = &bytes[..PAGE_HEADER_SIZE];
        let page_id = u64::from_le_bytes(header_bytes[0..8].try_into().map_err(|_| {
            SikioError::PageCorrupted {
                page_id: 0,
                reason: "Invalid page_id".into(),
            }
        })?);
        let page_type = header_bytes[8];
        let item_count = u16::from_le_bytes(header_bytes[9..11].try_into().map_err(|_| {
            SikioError::PageCorrupted {
                page_id: 0,
                reason: "Invalid item_count".into(),
            }
        })?);
        let free_space_offset =
            u16::from_le_bytes(header_bytes[11..13].try_into().map_err(|_| {
                SikioError::PageCorrupted {
                    page_id: 0,
                    reason: "Invalid free_space_offset".into(),
                }
            })?);
        let stored_checksum =
            u32::from_le_bytes(header_bytes[16..20].try_into().map_err(|_| {
                SikioError::PageCorrupted {
                    page_id: 0,
                    reason: "Invalid checksum bytes".into(),
                }
            })?);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes[0..16]);
        hasher.update(&[0u8; 4]);
        hasher.update(&bytes[20..]);
        let computed_checksum = hasher.finalize();
        if stored_checksum != 0 && stored_checksum != computed_checksum {
            return Err(SikioError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }
        let header = PageHeader {
            page_id,
            page_type,
            item_count,
            free_space_offset,
            checksum: stored_checksum,
        };
        let data = bytes[PAGE_HEADER_SIZE..].to_vec();
        Ok(Page { header, data })
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; PAGE_SIZE];
        bytes[0..8].copy_from_slice(&self.header.page_id.to_le_bytes());
        bytes[8] = self.header.page_type;
        bytes[9..11].copy_from_slice(&self.header.item_count.to_le_bytes());
        bytes[11..13].copy_from_slice(&self.header.free_space_offset.to_le_bytes());
        bytes[PAGE_HEADER_SIZE..].copy_from_slice(&self.data);
        let checksum = crc32fast::hash(&bytes);
        bytes[16..20].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }
    pub fn free_space(&self) -> usize {
        self.header.free_space_offset as usize
    }
}
#[derive(Debug, Clone)]
pub struct CellPointer {
    pub offset: u16,
    pub length: u16,
}
impl CellPointer {
    pub const SIZE: usize = 4;
    pub fn from_bytes(bytes: &[u8]) -> Self {
        CellPointer {
            offset: u16::from_le_bytes([bytes[0], bytes[1]]),
            length: u16::from_le_bytes([bytes[2], bytes[3]]),
        }
    }
    pub fn to_bytes(&self) -> [u8; 4] {
        let mut bytes = [0u8; 4];
        bytes[0..2].copy_from_slice(&self.offset.to_le_bytes());
        bytes[2..4].copy_from_slice(&self.length.to_le_bytes());
        bytes
    }
}
pub const OVERFLOW_HEADER_SIZE: usize = 24;
pub const OVERFLOW_DATA_SIZE: usize = PAGE_SIZE - OVERFLOW_HEADER_SIZE;
#[derive(Debug, Clone)]
pub struct OverflowPage {
    pub page_id: u64,
    pub next_page: u64,
    pub data_length: u32,
    pub checksum: u32,
    pub data: Vec<u8>,
}
impl OverflowPage {
    pub fn new(page_id: u64) -> Self {
        OverflowPage {
            page_id,
            next_page: 0,
            data_length: 0,
            checksum: 0,
            data: Vec::new(),
        }
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(SikioError::PageCorrupted {
                page_id: 0,
                reason: "Invalid overflow page size".into(),
            });
        }
        let page_id =
            u64::from_le_bytes(
                bytes[0..8]
                    .try_into()
                    .map_err(|_| SikioError::PageCorrupted {
                        page_id: 0,
                        reason: "Invalid overflow page_id".into(),
                    })?,
            );
        let next_page =
            u64::from_le_bytes(
                bytes[8..16]
                    .try_into()
                    .map_err(|_| SikioError::PageCorrupted {
                        page_id,
                        reason: "Invalid overflow next_page".into(),
                    })?,
            );
        let data_length = u32::from_le_bytes(bytes[16..20].try_into().map_err(|_| {
            SikioError::PageCorrupted {
                page_id,
                reason: "Invalid overflow data_length".into(),
            }
        })?);
        let stored_checksum = u32::from_le_bytes(bytes[20..24].try_into().map_err(|_| {
            SikioError::PageCorrupted {
                page_id,
                reason: "Invalid overflow checksum bytes".into(),
            }
        })?);
        if OVERFLOW_HEADER_SIZE + data_length as usize > PAGE_SIZE {
            return Err(SikioError::PageCorrupted {
                page_id,
                reason: "Overflow data_length exceeds page".into(),
            });
        }
        let data =
            bytes[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + data_length as usize].to_vec();
        if stored_checksum != 0 {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&bytes[0..20]);
            hasher.update(&data);
            let computed = hasher.finalize();
            if stored_checksum != computed {
                return Err(SikioError::ChecksumMismatch {
                    expected: stored_checksum,
                    actual: computed,
                });
            }
        }
        Ok(OverflowPage {
            page_id,
            next_page,
            data_length,
            checksum: stored_checksum,
            data,
        })
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; PAGE_SIZE];
        bytes[0..8].copy_from_slice(&self.page_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.next_page.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.data_length.to_le_bytes());
        bytes[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + self.data.len()]
            .copy_from_slice(&self.data);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes[0..20]);
        hasher.update(&self.data);
        let checksum = hasher.finalize();
        bytes[20..24].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }
    pub fn max_data_per_page() -> usize {
        OVERFLOW_DATA_SIZE
    }
}
pub fn validate_key_value(key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY_SIZE {
        return Err(SikioError::KeyTooLarge {
            max: MAX_KEY_SIZE,
            actual: key.len(),
        });
    }
    if value.len() > MAX_VALUE_SIZE {
        return Err(SikioError::ValueTooLarge {
            max: MAX_VALUE_SIZE,
            actual: value.len(),
        });
    }
    Ok(())
}
