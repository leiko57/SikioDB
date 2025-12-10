use crate::error::{Result, SikioError};
pub const WAL_ENTRY_HEADER_SIZE: usize = 24;
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
        let sequence = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let op_byte = bytes[8];
        let key_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        let expected_len = WAL_ENTRY_HEADER_SIZE + key_len + value_len;
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
        let key_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let entry_size = WAL_ENTRY_HEADER_SIZE + key_len + value_len;
        if self.position + entry_size > self.data.len() {
            return Some(Err(SikioError::WalCorrupted {
                sequence: 0,
                reason: "Truncated entry".into(),
            }));
        }
        let entry_bytes = &self.data[self.position..self.position + entry_size];
        self.position += entry_size;
        Some(WalEntry::from_bytes(entry_bytes))
    }
}
