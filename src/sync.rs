use crate::error::{Result, SikioError};
const LWW_HEADER_SIZE: usize = 12;
pub struct HybridLogicalClock {
    logical: u64,
    node_id: u32,
}
impl HybridLogicalClock {
    pub fn new(node_id: u32) -> Self {
        HybridLogicalClock {
            logical: js_sys::Date::now() as u64,
            node_id,
        }
    }
    pub fn tick(&mut self) -> u64 {
        let now = js_sys::Date::now() as u64;
        self.logical = self.logical.max(now) + 1;
        self.logical
    }
    pub fn update(&mut self, received: u64) {
        let now = js_sys::Date::now() as u64;
        self.logical = self.logical.max(now).max(received) + 1;
    }
    pub fn now(&self) -> u64 {
        self.logical
    }
    pub fn node_id(&self) -> u32 {
        self.node_id
    }
}
#[derive(Debug, Clone)]
pub struct LWWValue {
    pub data: Vec<u8>,
    pub hlc: u64,
    pub node_id: u32,
}
impl LWWValue {
    pub fn new(data: Vec<u8>, hlc: u64, node_id: u32) -> Self {
        LWWValue { data, hlc, node_id }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(LWW_HEADER_SIZE + self.data.len());
        bytes.extend_from_slice(&self.hlc.to_le_bytes());
        bytes.extend_from_slice(&self.node_id.to_le_bytes());
        bytes.extend_from_slice(&self.data);
        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < LWW_HEADER_SIZE {
            return Err(SikioError::Corrupted("LWW value too short".into()));
        }
        let hlc = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| SikioError::Corrupted("Invalid HLC".into()))?,
        );
        let node_id = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| SikioError::Corrupted("Invalid node_id".into()))?,
        );
        let data = bytes[LWW_HEADER_SIZE..].to_vec();
        Ok(LWWValue { data, hlc, node_id })
    }
    pub fn wins_over(&self, other: &LWWValue) -> bool {
        if self.hlc > other.hlc {
            return true;
        }
        if self.hlc == other.hlc && self.node_id > other.node_id {
            return true;
        }
        false
    }
}
#[derive(Debug, Clone)]
pub struct SyncDelta {
    pub entries: Vec<(Vec<u8>, LWWValue)>,
}
impl SyncDelta {
    pub fn new() -> Self {
        SyncDelta {
            entries: Vec::new(),
        }
    }
    pub fn add(&mut self, key: Vec<u8>, value: LWWValue) {
        self.entries.push((key, value));
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (key, value) in &self.entries {
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(key);
            let value_bytes = value.to_bytes();
            bytes.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&value_bytes);
        }
        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(SikioError::Corrupted("Delta too short".into()));
        }
        let count = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| SikioError::Corrupted("Invalid delta count".into()))?,
        ) as usize;
        let mut entries = Vec::with_capacity(count);
        let mut offset = 4;
        for _ in 0..count {
            if offset + 4 > bytes.len() {
                return Err(SikioError::Corrupted("Delta truncated".into()));
            }
            let key_len = u32::from_le_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .map_err(|_| SikioError::Corrupted("Invalid key len".into()))?,
            ) as usize;
            offset += 4;
            if offset + key_len > bytes.len() {
                return Err(SikioError::Corrupted("Key truncated".into()));
            }
            let key = bytes[offset..offset + key_len].to_vec();
            offset += key_len;
            if offset + 4 > bytes.len() {
                return Err(SikioError::Corrupted("Value len missing".into()));
            }
            let value_len = u32::from_le_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .map_err(|_| SikioError::Corrupted("Invalid value len".into()))?,
            ) as usize;
            offset += 4;
            if offset + value_len > bytes.len() {
                return Err(SikioError::Corrupted("Value truncated".into()));
            }
            let value = LWWValue::from_bytes(&bytes[offset..offset + value_len])?;
            offset += value_len;
            entries.push((key, value));
        }
        Ok(SyncDelta { entries })
    }
}
pub fn merge_lww(local: Option<&LWWValue>, remote: &LWWValue) -> bool {
    match local {
        Some(l) => remote.wins_over(l),
        None => true,
    }
}
