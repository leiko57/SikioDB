use std::fmt;
#[derive(Debug, Clone)]
pub enum SikioError {
    KeyNotFound,
    KeyTooLarge { max: usize, actual: usize },
    ValueTooLarge { max: usize, actual: usize },
    PageCorrupted { page_id: u64, reason: String },
    WalCorrupted { sequence: u64, reason: String },
    IoError(String),
    ChecksumMismatch { expected: u32, actual: u32 },
    StorageNotInitialized,
    PageCacheFull,
    BTreeOverflow,
    Corrupted(String),
}
impl fmt::Display for SikioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SikioError::KeyNotFound => write!(f, "Key not found"),
            SikioError::KeyTooLarge { max, actual } => {
                write!(f, "Key too large: {} bytes (max {})", actual, max)
            }
            SikioError::ValueTooLarge { max, actual } => {
                write!(f, "Value too large: {} bytes (max {})", actual, max)
            }
            SikioError::PageCorrupted { page_id, reason } => {
                write!(f, "Page {} corrupted: {}", page_id, reason)
            }
            SikioError::WalCorrupted { sequence, reason } => {
                write!(f, "WAL entry {} corrupted: {}", sequence, reason)
            }
            SikioError::IoError(msg) => write!(f, "IO error: {}", msg),
            SikioError::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "Checksum mismatch: expected {}, got {}",
                    expected, actual
                )
            }
            SikioError::StorageNotInitialized => write!(f, "Storage not initialized"),
            SikioError::PageCacheFull => write!(f, "Page cache full"),
            SikioError::BTreeOverflow => write!(f, "B-Tree node overflow"),
            SikioError::Corrupted(msg) => write!(f, "Data corrupted: {}", msg),
        }
    }
}
impl std::error::Error for SikioError {}
pub type Result<T> = std::result::Result<T, SikioError>;
