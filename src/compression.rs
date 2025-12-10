#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
}
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub threshold: usize,
}
impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            threshold: 128,
        }
    }
}
impl CompressionConfig {
    pub fn none() -> Self {
        CompressionConfig {
            algorithm: CompressionAlgorithm::None,
            threshold: usize::MAX,
        }
    }
    pub fn lz4(threshold: usize) -> Self {
        CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            threshold,
        }
    }
}
pub fn compress_with_config(data: &[u8], config: &CompressionConfig) -> Vec<u8> {
    if data.len() < config.threshold {
        let mut result = Vec::with_capacity(1 + data.len());
        result.push(0x00);
        result.extend_from_slice(data);
        return result;
    }
    match config.algorithm {
        CompressionAlgorithm::None => {
            let mut result = Vec::with_capacity(1 + data.len());
            result.push(0x00);
            result.extend_from_slice(data);
            result
        }
        CompressionAlgorithm::Lz4 => {
            let compressed = lz4_flex::compress_prepend_size(data);
            if compressed.len() < data.len() {
                let mut result = Vec::with_capacity(1 + compressed.len());
                result.push(0x01);
                result.extend_from_slice(&compressed);
                result
            } else {
                let mut result = Vec::with_capacity(1 + data.len());
                result.push(0x00);
                result.extend_from_slice(data);
                result
            }
        }
    }
}
pub fn compress(data: &[u8]) -> Vec<u8> {
    if data.len() < 128 {
        return data.to_vec();
    }
    let compressed = lz4_flex::compress_prepend_size(data);
    if compressed.len() < data.len() {
        let mut result = Vec::with_capacity(1 + compressed.len());
        result.push(0x01);
        result.extend_from_slice(&compressed);
        result
    } else {
        let mut result = Vec::with_capacity(1 + data.len());
        result.push(0x00);
        result.extend_from_slice(data);
        result
    }
}
pub fn decompress(data: &[u8]) -> Option<Vec<u8>> {
    if data.is_empty() {
        return Some(Vec::new());
    }
    match data[0] {
        0x00 => Some(data[1..].to_vec()),
        0x01 => lz4_flex::decompress_size_prepended(&data[1..]).ok(),
        _ => None,
    }
}
pub fn compress_value(value: &[u8], threshold: usize) -> Vec<u8> {
    if value.len() < threshold {
        value.to_vec()
    } else {
        compress(value)
    }
}
pub fn decompress_value(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    if data[0] == 0x00 || data[0] == 0x01 {
        decompress(data).unwrap_or_else(|| data.to_vec())
    } else {
        data.to_vec()
    }
}
