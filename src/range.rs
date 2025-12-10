#[derive(Debug, Clone)]
pub enum RangeBound {
    Unbounded,
    Included(Vec<u8>),
    Excluded(Vec<u8>),
}
impl RangeBound {
    pub fn included(key: impl Into<Vec<u8>>) -> Self {
        RangeBound::Included(key.into())
    }
    pub fn excluded(key: impl Into<Vec<u8>>) -> Self {
        RangeBound::Excluded(key.into())
    }
    pub fn is_before(&self, key: &[u8]) -> bool {
        match self {
            RangeBound::Unbounded => true,
            RangeBound::Included(bound) => key >= bound.as_slice(),
            RangeBound::Excluded(bound) => key > bound.as_slice(),
        }
    }
    pub fn is_after(&self, key: &[u8]) -> bool {
        match self {
            RangeBound::Unbounded => true,
            RangeBound::Included(bound) => key <= bound.as_slice(),
            RangeBound::Excluded(bound) => key < bound.as_slice(),
        }
    }
    pub fn start_key(&self) -> Option<&[u8]> {
        match self {
            RangeBound::Unbounded => None,
            RangeBound::Included(k) | RangeBound::Excluded(k) => Some(k),
        }
    }
}
#[derive(Debug)]
pub struct KeyValuePair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
impl KeyValuePair {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        KeyValuePair { key, value }
    }
}
pub fn key_in_range(key: &[u8], start: &RangeBound, end: &RangeBound) -> bool {
    start.is_before(key) && end.is_after(key)
}
pub fn key_matches_prefix(key: &[u8], prefix: &[u8]) -> bool {
    key.len() >= prefix.len() && &key[..prefix.len()] == prefix
}
pub fn prefix_to_range(prefix: &[u8]) -> (RangeBound, RangeBound) {
    let start = RangeBound::Included(prefix.to_vec());
    let mut end_key = prefix.to_vec();
    let mut i = end_key.len();
    while i > 0 {
        i -= 1;
        if end_key[i] < 0xFF {
            end_key[i] += 1;
            end_key.truncate(i + 1);
            return (start, RangeBound::Excluded(end_key));
        }
    }
    (start, RangeBound::Unbounded)
}
