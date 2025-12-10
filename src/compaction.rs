use crate::btree::BTree;
use crate::page::PAGE_SIZE;
pub struct CompactionStats {
    pub pages_scanned: u64,
    pub pages_freed: u64,
    pub bytes_reclaimed: u64,
}
impl CompactionStats {
    pub fn new() -> Self {
        CompactionStats {
            pages_scanned: 0,
            pages_freed: 0,
            bytes_reclaimed: 0,
        }
    }
}
pub fn estimate_fragmentation(btree: &BTree) -> f64 {
    let total_pages = btree.next_page_id();
    let free_pages = btree.free_page_ids().len() as u64;
    if total_pages == 0 {
        return 0.0;
    }
    (free_pages as f64 / total_pages as f64) * 100.0
}
pub fn reclaim_free_pages(btree: &mut BTree) -> CompactionStats {
    let mut stats = CompactionStats::new();
    let free_count = btree.free_page_ids().len();
    stats.pages_freed = free_count as u64;
    stats.bytes_reclaimed = free_count as u64 * PAGE_SIZE as u64;
    stats
}
pub fn should_compact(btree: &BTree, threshold_percent: f64) -> bool {
    estimate_fragmentation(btree) > threshold_percent
}
pub struct VacuumConfig {
    pub fragmentation_threshold: f64,
    pub max_pages_per_run: u64,
}
impl Default for VacuumConfig {
    fn default() -> Self {
        VacuumConfig {
            fragmentation_threshold: 20.0,
            max_pages_per_run: 1000,
        }
    }
}
