use std::sync::atomic::{AtomicU64, Ordering};
pub struct DatabaseStats {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub page_reads: AtomicU64,
    pub page_writes: AtomicU64,
    pub wal_writes: AtomicU64,
    pub compactions: AtomicU64,
}
impl DatabaseStats {
    pub fn new() -> Self {
        DatabaseStats {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            page_reads: AtomicU64::new(0),
            page_writes: AtomicU64::new(0),
            wal_writes: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
        }
    }
    pub fn record_read(&self, bytes: u64) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn record_write(&self, bytes: u64) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_page_read(&self) {
        self.page_reads.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_page_write(&self) {
        self.page_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_wal_write(&self) {
        self.wal_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_compaction(&self) {
        self.compactions.fetch_add(1, Ordering::Relaxed);
    }
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            return 0.0;
        }
        (hits as f64 / total as f64) * 100.0
    }
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"reads":{},"writes":{},"deletes":{},"cache_hits":{},"cache_misses":{},"cache_hit_rate":{:.2},"bytes_read":{},"bytes_written":{},"page_reads":{},"page_writes":{},"wal_writes":{},"compactions":{}}}"#,
            self.reads.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
            self.deletes.load(Ordering::Relaxed),
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
            self.cache_hit_rate(),
            self.bytes_read.load(Ordering::Relaxed),
            self.bytes_written.load(Ordering::Relaxed),
            self.page_reads.load(Ordering::Relaxed),
            self.page_writes.load(Ordering::Relaxed),
            self.wal_writes.load(Ordering::Relaxed),
            self.compactions.load(Ordering::Relaxed),
        )
    }
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        output.push_str("# HELP sikiodb_reads_total Total number of read operations\n");
        output.push_str("# TYPE sikiodb_reads_total counter\n");
        output.push_str(&format!(
            "sikiodb_reads_total {}\n",
            self.reads.load(Ordering::Relaxed)
        ));
        output.push_str("# HELP sikiodb_writes_total Total number of write operations\n");
        output.push_str("# TYPE sikiodb_writes_total counter\n");
        output.push_str(&format!(
            "sikiodb_writes_total {}\n",
            self.writes.load(Ordering::Relaxed)
        ));
        output.push_str("# HELP sikiodb_cache_hit_rate Cache hit rate percentage\n");
        output.push_str("# TYPE sikiodb_cache_hit_rate gauge\n");
        output.push_str(&format!(
            "sikiodb_cache_hit_rate {:.2}\n",
            self.cache_hit_rate()
        ));
        output.push_str("# HELP sikiodb_bytes_written_total Total bytes written\n");
        output.push_str("# TYPE sikiodb_bytes_written_total counter\n");
        output.push_str(&format!(
            "sikiodb_bytes_written_total {}\n",
            self.bytes_written.load(Ordering::Relaxed)
        ));
        output
    }
    pub fn reset(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.page_reads.store(0, Ordering::Relaxed);
        self.page_writes.store(0, Ordering::Relaxed);
        self.wal_writes.store(0, Ordering::Relaxed);
        self.compactions.store(0, Ordering::Relaxed);
    }
}
impl Default for DatabaseStats {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}
impl From<&DatabaseStats> for StatsSnapshot {
    fn from(stats: &DatabaseStats) -> Self {
        StatsSnapshot {
            reads: stats.reads.load(Ordering::Relaxed),
            writes: stats.writes.load(Ordering::Relaxed),
            deletes: stats.deletes.load(Ordering::Relaxed),
            cache_hits: stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: stats.cache_misses.load(Ordering::Relaxed),
            bytes_read: stats.bytes_read.load(Ordering::Relaxed),
            bytes_written: stats.bytes_written.load(Ordering::Relaxed),
        }
    }
}
