use crate::error::{Result, SikioError};
use std::collections::BTreeMap;
pub struct SecondaryIndex {
    name: String,
    entries: BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
}
impl SecondaryIndex {
    pub fn new(name: &str) -> Self {
        SecondaryIndex {
            name: name.to_string(),
            entries: BTreeMap::new(),
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn insert(&mut self, indexed_value: Vec<u8>, primary_key: Vec<u8>) {
        self.entries
            .entry(indexed_value)
            .or_insert_with(Vec::new)
            .push(primary_key);
    }
    pub fn remove(&mut self, indexed_value: &[u8], primary_key: &[u8]) -> bool {
        if let Some(keys) = self.entries.get_mut(indexed_value) {
            if let Some(pos) = keys.iter().position(|k| k == primary_key) {
                keys.remove(pos);
                if keys.is_empty() {
                    self.entries.remove(indexed_value);
                }
                return true;
            }
        }
        false
    }
    pub fn get(&self, indexed_value: &[u8]) -> Option<&Vec<Vec<u8>>> {
        self.entries.get(indexed_value)
    }
    pub fn range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = (&Vec<u8>, &Vec<Vec<u8>>)> {
        self.entries.range(start.to_vec()..=end.to_vec())
    }
    pub fn clear(&mut self) {
        self.entries.clear();
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let name_bytes = self.name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name_bytes);
        bytes.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (indexed_val, primary_keys) in &self.entries {
            bytes.extend_from_slice(&(indexed_val.len() as u32).to_le_bytes());
            bytes.extend_from_slice(indexed_val);
            bytes.extend_from_slice(&(primary_keys.len() as u32).to_le_bytes());
            for pk in primary_keys {
                bytes.extend_from_slice(&(pk.len() as u32).to_le_bytes());
                bytes.extend_from_slice(pk);
            }
        }
        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut offset = 0;
        if bytes.len() < 4 {
            return Err(SikioError::Corrupted("Index too short".into()));
        }
        let name_len = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| SikioError::Corrupted("Invalid name len".into()))?,
        ) as usize;
        offset += 4;
        if offset + name_len > bytes.len() {
            return Err(SikioError::Corrupted("Name truncated".into()));
        }
        let name = String::from_utf8_lossy(&bytes[offset..offset + name_len]).to_string();
        offset += name_len;
        if offset + 4 > bytes.len() {
            return Err(SikioError::Corrupted("Entry count missing".into()));
        }
        let entry_count = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| SikioError::Corrupted("Invalid entry count".into()))?,
        ) as usize;
        offset += 4;
        let mut entries = BTreeMap::new();
        for _ in 0..entry_count {
            if offset + 4 > bytes.len() {
                return Err(SikioError::Corrupted("Indexed value len missing".into()));
            }
            let iv_len = u32::from_le_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .map_err(|_| SikioError::Corrupted("Invalid iv len".into()))?,
            ) as usize;
            offset += 4;
            if offset + iv_len > bytes.len() {
                return Err(SikioError::Corrupted("Indexed value truncated".into()));
            }
            let indexed_value = bytes[offset..offset + iv_len].to_vec();
            offset += iv_len;
            if offset + 4 > bytes.len() {
                return Err(SikioError::Corrupted("PK count missing".into()));
            }
            let pk_count = u32::from_le_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .map_err(|_| SikioError::Corrupted("Invalid pk count".into()))?,
            ) as usize;
            offset += 4;
            let mut primary_keys = Vec::with_capacity(pk_count);
            for _ in 0..pk_count {
                if offset + 4 > bytes.len() {
                    return Err(SikioError::Corrupted("PK len missing".into()));
                }
                let pk_len = u32::from_le_bytes(
                    bytes[offset..offset + 4]
                        .try_into()
                        .map_err(|_| SikioError::Corrupted("Invalid pk len".into()))?,
                ) as usize;
                offset += 4;
                if offset + pk_len > bytes.len() {
                    return Err(SikioError::Corrupted("PK truncated".into()));
                }
                primary_keys.push(bytes[offset..offset + pk_len].to_vec());
                offset += pk_len;
            }
            entries.insert(indexed_value, primary_keys);
        }
        Ok(SecondaryIndex { name, entries })
    }
}
pub struct IndexRegistry {
    indexes: BTreeMap<String, SecondaryIndex>,
}
impl IndexRegistry {
    pub fn new() -> Self {
        IndexRegistry {
            indexes: BTreeMap::new(),
        }
    }
    pub fn create_index(&mut self, name: &str) -> &mut SecondaryIndex {
        self.indexes
            .entry(name.to_string())
            .or_insert_with(|| SecondaryIndex::new(name))
    }
    pub fn get_index(&self, name: &str) -> Option<&SecondaryIndex> {
        self.indexes.get(name)
    }
    pub fn get_index_mut(&mut self, name: &str) -> Option<&mut SecondaryIndex> {
        self.indexes.get_mut(name)
    }
    pub fn drop_index(&mut self, name: &str) -> bool {
        self.indexes.remove(name).is_some()
    }
    pub fn list_indexes(&self) -> Vec<&str> {
        self.indexes.keys().map(|s| s.as_str()).collect()
    }
}
