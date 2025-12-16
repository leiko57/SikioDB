use crate::error::{Result, SikioError};
pub const WAL_ENTRY_HEADER_SIZE: usize = 24;
const SEQUENCE_START: usize = 0;
const SEQUENCE_END: usize = 8;
const OPERATION_OFFSET: usize = 8;
const KEY_LEN_START: usize = 12;
const KEY_LEN_END: usize = 16;
const VALUE_LEN_START: usize = 16;
const VALUE_LEN_END: usize = 20;
const CHECKSUM_START: usize = 20;
const CHECKSUM_END: usize = 24;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOperation {
    Put,
    Delete,
    Commit,
    Checkpoint,
}
impl From<WalOperation> for u8 {
    fn from(op: WalOperation) -> Self {
        match op {
            WalOperation::Put => 1,
            WalOperation::Delete => 2,
            WalOperation::Commit => 3,
            WalOperation::Checkpoint => 4,
        }
    }
}
impl TryFrom<u8> for WalOperation {
    type Error = SikioError;
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(WalOperation::Put),
            2 => Ok(WalOperation::Delete),
            3 => Ok(WalOperation::Commit),
            4 => Ok(WalOperation::Checkpoint),
            _ => Err(SikioError::WalCorrupted {
                sequence: 0,
                reason: format!("Unknown operation: {}", value),
            }),
        }
    }
}
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub sequence: u64,
    pub operation: WalOperation,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub checksum: u32,
}
impl WalEntry {
    pub fn new_put(sequence: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        let mut entry = WalEntry {
            sequence,
            operation: WalOperation::Put,
            key,
            value: Some(value),
            checksum: 0,
        };
        entry.compute_checksum();
        entry
    }
    pub fn new_delete(sequence: u64, key: Vec<u8>) -> Self {
        let mut entry = WalEntry {
            sequence,
            operation: WalOperation::Delete,
            key,
            value: None,
            checksum: 0,
        };
        entry.compute_checksum();
        entry
    }
    pub fn new_commit(sequence: u64) -> Self {
        let mut entry = WalEntry {
            sequence,
            operation: WalOperation::Commit,
            key: Vec::new(),
            value: None,
            checksum: 0,
        };
        entry.compute_checksum();
        entry
    }
    pub fn new_checkpoint(sequence: u64) -> Self {
        let mut entry = WalEntry {
            sequence,
            operation: WalOperation::Checkpoint,
            key: Vec::new(),
            value: None,
            checksum: 0,
        };
        entry.compute_checksum();
        entry
    }
    fn compute_checksum(&mut self) {
        self.checksum = Self::calculate_checksum(
            self.sequence,
            self.operation,
            &self.key,
            self.value.as_deref(),
        );
    }
    fn calculate_checksum(
        sequence: u64,
        operation: WalOperation,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&sequence.to_le_bytes());
        hasher.update(&[u8::from(operation)]);
        hasher.update(&(key.len() as u32).to_le_bytes());
        hasher.update(key);
        let value_len = value.map(|v| v.len()).unwrap_or(0) as u32;
        hasher.update(&value_len.to_le_bytes());
        if let Some(v) = value {
            hasher.update(v);
        }
        hasher.finalize()
    }
    pub fn verify_checksum(&self) -> bool {
        let computed = Self::calculate_checksum(
            self.sequence,
            self.operation,
            &self.key,
            self.value.as_deref(),
        );
        computed == self.checksum
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let value_len = self.value.as_ref().map(|v| v.len()).unwrap_or(0) as u32;
        let total_len = WAL_ENTRY_HEADER_SIZE + self.key.len() + value_len as usize;
        let mut bytes = Vec::with_capacity(total_len);
        self.write_to_buffer(&mut bytes);
        bytes
    }
    pub fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.sequence.to_le_bytes());
        buffer.push(u8::from(self.operation));
        buffer.extend_from_slice(&[0u8; 3]);
        buffer.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        let value_len = self.value.as_ref().map(|v| v.len()).unwrap_or(0) as u32;
        buffer.extend_from_slice(&value_len.to_le_bytes());
        buffer.extend_from_slice(&self.checksum.to_le_bytes());
        buffer.extend_from_slice(&self.key);
        if let Some(ref v) = self.value {
            buffer.extend_from_slice(v);
        }
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < WAL_ENTRY_HEADER_SIZE {
            return Err(SikioError::WalCorrupted {
                sequence: 0,
                reason: "Entry too short".into(),
            });
        }
        let sequence = u64::from_le_bytes(
            bytes[SEQUENCE_START..SEQUENCE_END]
                .try_into()
                .map_err(|_| SikioError::WalCorrupted {
                    sequence: 0,
                    reason: "Invalid sequence bytes".into(),
                })?,
        );
        let op_byte = bytes[OPERATION_OFFSET];
        let key_len = u32::from_le_bytes(
            bytes[KEY_LEN_START..KEY_LEN_END]
                .try_into()
                .map_err(|_| SikioError::WalCorrupted {
                    sequence,
                    reason: "Invalid key length bytes".into(),
                })?,
        ) as usize;
        let value_len = u32::from_le_bytes(
            bytes[VALUE_LEN_START..VALUE_LEN_END]
                .try_into()
                .map_err(|_| SikioError::WalCorrupted {
                    sequence,
                    reason: "Invalid value length bytes".into(),
                })?,
        ) as usize;
        let checksum = u32::from_le_bytes(
            bytes[CHECKSUM_START..CHECKSUM_END]
                .try_into()
                .map_err(|_| SikioError::WalCorrupted {
                    sequence,
                    reason: "Invalid checksum bytes".into(),
                })?,
        );
        let expected_len =
            WAL_ENTRY_HEADER_SIZE
                .checked_add(key_len)
                .and_then(|v| v.checked_add(value_len))
                .ok_or_else(|| SikioError::WalCorrupted {
                    sequence,
                    reason: "Entry size overflow".into(),
                })?;
        if bytes.len() < expected_len {
            return Err(SikioError::WalCorrupted {
                sequence,
                reason: format!("Truncated entry: {} < {}", bytes.len(), expected_len),
            });
        }
        let operation = WalOperation::try_from(op_byte).map_err(|_| SikioError::WalCorrupted {
            sequence,
            reason: format!("Unknown operation: {}", op_byte),
        })?;
        match operation {
            WalOperation::Put => {
                if value_len == 0 {
                    return Err(SikioError::WalCorrupted {
                        sequence,
                        reason: "Put entry missing value".into(),
                    });
                }
            }
            WalOperation::Delete => {
                if value_len != 0 {
                    return Err(SikioError::WalCorrupted {
                        sequence,
                        reason: "Delete entry has unexpected value".into(),
                    });
                }
            }
            WalOperation::Commit | WalOperation::Checkpoint => {
                if key_len != 0 || value_len != 0 {
                    return Err(SikioError::WalCorrupted {
                        sequence,
                        reason: "Control entry has unexpected payload".into(),
                    });
                }
            }
        }
        let key_start = WAL_ENTRY_HEADER_SIZE;
        let key = bytes[key_start..key_start + key_len].to_vec();
        let value = if value_len > 0 {
            let value_start = key_start + key_len;
            Some(bytes[value_start..value_start + value_len].to_vec())
        } else {
            None
        };
        let entry = WalEntry {
            sequence,
            operation,
            key,
            value,
            checksum,
        };
        if !entry.verify_checksum() {
            return Err(SikioError::WalCorrupted {
                sequence,
                reason: "Checksum mismatch".into(),
            });
        }
        Ok(entry)
    }
    pub fn serialize_put(sequence: u64, key: &[u8], value: &[u8], buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&sequence.to_le_bytes());
        buffer.push(u8::from(WalOperation::Put));
        buffer.extend_from_slice(&[0u8; 3]);
        buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
        let checksum_offset = buffer.len();
        buffer.extend_from_slice(&[0u8; 4]);
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(value);
        let checksum = Self::calculate_checksum(sequence, WalOperation::Put, key, Some(value));
        let checksum_bytes = checksum.to_le_bytes();
        buffer[checksum_offset] = checksum_bytes[0];
        buffer[checksum_offset + 1] = checksum_bytes[1];
        buffer[checksum_offset + 2] = checksum_bytes[2];
        buffer[checksum_offset + 3] = checksum_bytes[3];
    }
    pub fn serialize_commit(sequence: u64, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&sequence.to_le_bytes());
        buffer.push(u8::from(WalOperation::Commit));
        buffer.extend_from_slice(&[0u8; 3]);
        buffer.extend_from_slice(&0u32.to_le_bytes());
        buffer.extend_from_slice(&0u32.to_le_bytes());
        let checksum_offset = buffer.len();
        buffer.extend_from_slice(&[0u8; 4]);
        let checksum = Self::calculate_checksum(sequence, WalOperation::Commit, &[], None);
        let checksum_bytes = checksum.to_le_bytes();
        buffer[checksum_offset] = checksum_bytes[0];
        buffer[checksum_offset + 1] = checksum_bytes[1];
        buffer[checksum_offset + 2] = checksum_bytes[2];
        buffer[checksum_offset + 3] = checksum_bytes[3];
    }
    pub fn serialize_checkpoint(sequence: u64, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&sequence.to_le_bytes());
        buffer.push(u8::from(WalOperation::Checkpoint));
        buffer.extend_from_slice(&[0u8; 3]);
        buffer.extend_from_slice(&0u32.to_le_bytes());
        buffer.extend_from_slice(&0u32.to_le_bytes());
        let checksum_offset = buffer.len();
        buffer.extend_from_slice(&[0u8; 4]);
        let checksum = Self::calculate_checksum(sequence, WalOperation::Checkpoint, &[], None);
        let checksum_bytes = checksum.to_le_bytes();
        buffer[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum_bytes);
    }
    pub fn serialize_delete(sequence: u64, key: &[u8], buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&sequence.to_le_bytes());
        buffer.push(u8::from(WalOperation::Delete));
        buffer.extend_from_slice(&[0u8; 3]);
        buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&0u32.to_le_bytes());
        let checksum_offset = buffer.len();
        buffer.extend_from_slice(&[0u8; 4]);
        buffer.extend_from_slice(key);
        let checksum = Self::calculate_checksum(sequence, WalOperation::Delete, key, None);
        let checksum_bytes = checksum.to_le_bytes();
        buffer[checksum_offset] = checksum_bytes[0];
        buffer[checksum_offset + 1] = checksum_bytes[1];
        buffer[checksum_offset + 2] = checksum_bytes[2];
        buffer[checksum_offset + 3] = checksum_bytes[3];
    }
}
pub struct WalReader<'a> {
    data: &'a [u8],
    position: usize,
}
impl<'a> WalReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        WalReader { data, position: 0 }
    }
}
impl<'a> Iterator for WalReader<'a> {
    type Item = Result<WalEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.data.len() {
            return None;
        }
        if self.position + WAL_ENTRY_HEADER_SIZE > self.data.len() {
            return Some(Err(SikioError::WalCorrupted {
                sequence: 0,
                reason: "Truncated header".into(),
            }));
        }
        let header = &self.data[self.position..self.position + WAL_ENTRY_HEADER_SIZE];
        let sequence = match header[SEQUENCE_START..SEQUENCE_END].try_into() {
            Ok(b) => u64::from_le_bytes(b),
            Err(_) => {
                return Some(Err(SikioError::WalCorrupted {
                    sequence: 0,
                    reason: "Invalid sequence bytes".into(),
                }))
            }
        };
        let key_len = match header[KEY_LEN_START..KEY_LEN_END].try_into() {
            Ok(b) => u32::from_le_bytes(b) as usize,
            Err(_) => {
                return Some(Err(SikioError::WalCorrupted {
                    sequence,
                    reason: "Invalid key length bytes".into(),
                }))
            }
        };
        let value_len = match header[VALUE_LEN_START..VALUE_LEN_END].try_into() {
            Ok(b) => u32::from_le_bytes(b) as usize,
            Err(_) => {
                return Some(Err(SikioError::WalCorrupted {
                    sequence,
                    reason: "Invalid value length bytes".into(),
                }))
            }
        };
        let entry_size = match WAL_ENTRY_HEADER_SIZE
            .checked_add(key_len)
            .and_then(|v| v.checked_add(value_len))
        {
            Some(v) => v,
            None => {
                return Some(Err(SikioError::WalCorrupted {
                    sequence,
                    reason: "Entry size overflow".into(),
                }))
            }
        };
        if self.position + entry_size > self.data.len() {
            return Some(Err(SikioError::WalCorrupted {
                sequence,
                reason: "Truncated entry".into(),
            }));
        }
        let entry_bytes = &self.data[self.position..self.position + entry_size];
        self.position += entry_size;
        Some(WalEntry::from_bytes(entry_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_put_entry() {
        let entry = WalEntry::new_put(7, b"k".to_vec(), b"v".to_vec());
        let bytes = entry.to_bytes();
        let decoded = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.sequence, 7);
        assert_eq!(decoded.operation, WalOperation::Put);
        assert_eq!(decoded.key, b"k".to_vec());
        assert_eq!(decoded.value, Some(b"v".to_vec()));
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn serialize_helpers_roundtrip() {
        let mut buf = Vec::new();
        WalEntry::serialize_put(1, b"a", b"b", &mut buf);
        let e1 = WalEntry::from_bytes(&buf).unwrap();
        assert_eq!(e1.operation, WalOperation::Put);
        assert_eq!(e1.sequence, 1);

        buf.clear();
        WalEntry::serialize_delete(2, b"a", &mut buf);
        let e2 = WalEntry::from_bytes(&buf).unwrap();
        assert_eq!(e2.operation, WalOperation::Delete);
        assert_eq!(e2.sequence, 2);
        assert_eq!(e2.value, None);

        buf.clear();
        WalEntry::serialize_commit(3, &mut buf);
        let e3 = WalEntry::from_bytes(&buf).unwrap();
        assert_eq!(e3.operation, WalOperation::Commit);
        assert_eq!(e3.sequence, 3);

        buf.clear();
        WalEntry::serialize_checkpoint(4, &mut buf);
        let e4 = WalEntry::from_bytes(&buf).unwrap();
        assert_eq!(e4.operation, WalOperation::Checkpoint);
        assert_eq!(e4.sequence, 4);
    }

    #[test]
    fn detects_checksum_mismatch() {
        let entry = WalEntry::new_put(10, b"k".to_vec(), b"v".to_vec());
        let mut bytes = entry.to_bytes();
        bytes[WAL_ENTRY_HEADER_SIZE] ^= 0x01;
        let err = WalEntry::from_bytes(&bytes).unwrap_err();
        match err {
            SikioError::WalCorrupted { sequence, reason } => {
                assert_eq!(sequence, 10);
                assert_eq!(reason, "Checksum mismatch");
            }
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn rejects_truncated_entry_without_panic() {
        let mut bytes = vec![0u8; WAL_ENTRY_HEADER_SIZE + 1];
        bytes[SEQUENCE_START..SEQUENCE_END].copy_from_slice(&1u64.to_le_bytes());
        bytes[OPERATION_OFFSET] = u8::from(WalOperation::Put);
        bytes[KEY_LEN_START..KEY_LEN_END].copy_from_slice(&10u32.to_le_bytes());
        bytes[VALUE_LEN_START..VALUE_LEN_END].copy_from_slice(&1u32.to_le_bytes());
        let err = WalEntry::from_bytes(&bytes).unwrap_err();
        match err {
            SikioError::WalCorrupted { sequence, .. } => assert_eq!(sequence, 1),
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn wal_reader_iterates_entries() {
        let mut wal = Vec::new();
        WalEntry::serialize_put(1, b"k1", b"v1", &mut wal);
        WalEntry::serialize_commit(2, &mut wal);
        WalEntry::serialize_delete(3, b"k2", &mut wal);
        WalEntry::serialize_commit(4, &mut wal);

        let reader = WalReader::new(&wal);
        let ops: Vec<_> = reader.map(|r| r.unwrap().operation).collect();
        assert_eq!(
            ops,
            vec![
                WalOperation::Put,
                WalOperation::Commit,
                WalOperation::Delete,
                WalOperation::Commit
            ]
        );
    }
}
