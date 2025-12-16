use crate::page::Page;
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
const DEFAULT_CACHE_SIZE: usize = 256;
#[derive(Debug, Clone, Default)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}
impl CacheMetrics {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}
#[derive(Debug)]
struct CacheEntry {
    page: Page,
    dirty: bool,
}
pub struct PageCache {
    pages: LruCache<u64, CacheEntry>,
    spilled: HashMap<u64, CacheEntry>,
    dirty_set: HashSet<u64>,
    metrics: CacheMetrics,
}
impl PageCache {
    pub fn new() -> Self {
        PageCache::with_capacity(DEFAULT_CACHE_SIZE)
    }
    pub fn with_capacity(max_pages: usize) -> Self {
        let cap =
            NonZeroUsize::new(max_pages).unwrap_or(NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap());
        PageCache {
            pages: LruCache::new(cap),
            spilled: HashMap::new(),
            dirty_set: HashSet::new(),
            metrics: CacheMetrics::default(),
        }
    }
    pub fn get(&mut self, page_id: u64) -> Option<&Page> {
        if let Some(entry) = self.pages.get(&page_id) {
            self.metrics.hits += 1;
            return Some(&entry.page);
        }
        if let Some(entry) = self.spilled.get(&page_id) {
            self.metrics.hits += 1;
            return Some(&entry.page);
        }
        self.metrics.misses += 1;
        None
    }
    pub fn get_mut(&mut self, page_id: u64) -> Option<&mut Page> {
        if let Some(entry) = self.pages.get_mut(&page_id) {
            if !entry.dirty {
                entry.dirty = true;
                self.dirty_set.insert(page_id);
            }
            Some(&mut entry.page)
        } else if let Some(entry) = self.spilled.get_mut(&page_id) {
            if !entry.dirty {
                entry.dirty = true;
                self.dirty_set.insert(page_id);
            }
            Some(&mut entry.page)
        } else {
            None
        }
    }
    pub fn insert(&mut self, page: Page, dirty: bool) -> Option<(u64, Page)> {
        let page_id = page.header.page_id;
        self.spilled.remove(&page_id);
        if dirty {
            self.dirty_set.insert(page_id);
        } else {
            self.dirty_set.remove(&page_id);
        }
        let entry = CacheEntry { page, dirty };
        if let Some((out_id, out_entry)) = self.pages.push(page_id, entry) {
            if out_id != page_id {
                self.metrics.evictions += 1;
                if out_entry.dirty {
                    self.dirty_set.insert(out_id);
                    let page_copy = out_entry.page.clone();
                    self.spilled.insert(out_id, out_entry);
                    return Some((out_id, page_copy));
                }
                self.dirty_set.remove(&out_id);
                return Some((out_id, out_entry.page));
            } else {
                return None;
            }
        }
        None
    }
    pub fn mark_dirty(&mut self, page_id: u64) {
        if let Some(entry) = self.pages.get_mut(&page_id) {
            if !entry.dirty {
                entry.dirty = true;
                self.dirty_set.insert(page_id);
            }
        } else if let Some(entry) = self.spilled.get_mut(&page_id) {
            if !entry.dirty {
                entry.dirty = true;
                self.dirty_set.insert(page_id);
            }
        }
    }
    pub fn mark_clean(&mut self, page_id: u64) {
        if let Some(entry) = self.pages.get_mut(&page_id) {
            if entry.dirty {
                entry.dirty = false;
                self.dirty_set.remove(&page_id);
            }
        } else if let Some(entry) = self.spilled.get_mut(&page_id) {
            if entry.dirty {
                entry.dirty = false;
                self.dirty_set.remove(&page_id);
            }
        }
    }
    pub fn is_dirty(&self, page_id: u64) -> bool {
        self.dirty_set.contains(&page_id)
    }
    pub fn dirty_pages(&self) -> Vec<u64> {
        self.dirty_set.iter().copied().collect()
    }
    pub fn dirty_page_count(&self) -> usize {
        self.dirty_set.len()
    }
    pub fn take_dirty_page(&mut self, page_id: u64) -> Option<Page> {
        if let Some(entry) = self.pages.get_mut(&page_id) {
            if entry.dirty {
                entry.dirty = false;
                self.dirty_set.remove(&page_id);
                return Some(entry.page.clone());
            }
        } else if let Some(entry) = self.spilled.get_mut(&page_id) {
            if entry.dirty {
                entry.dirty = false;
                self.dirty_set.remove(&page_id);
                return Some(entry.page.clone());
            }
        }
        None
    }
    pub fn contains(&self, page_id: u64) -> bool {
        self.pages.contains(&page_id) || self.spilled.contains_key(&page_id)
    }
    pub fn remove(&mut self, page_id: u64) -> Option<Page> {
        if let Some(entry) = self.pages.pop(&page_id) {
            if entry.dirty {
                self.dirty_set.remove(&page_id);
            }
            Some(entry.page)
        } else if let Some(entry) = self.spilled.remove(&page_id) {
            if entry.dirty {
                self.dirty_set.remove(&page_id);
            }
            Some(entry.page)
        } else {
            None
        }
    }
    pub fn clear_dirty(&mut self) {
        for entry in self.pages.iter_mut() {
            entry.1.dirty = false;
        }
        self.spilled.clear();
        self.dirty_set.clear();
    }
    pub fn clear(&mut self) -> Vec<(u64, Page)> {
        let dirty: Vec<_> = self
            .pages
            .iter()
            .filter(|(_, e)| e.dirty)
            .map(|(id, e)| (*id, e.page.clone()))
            .collect();
        let mut spilled_dirty: Vec<_> = self
            .spilled
            .iter()
            .filter(|(_, e)| e.dirty)
            .map(|(id, e)| (*id, e.page.clone()))
            .collect();
        self.pages.clear();
        self.spilled.clear();
        self.dirty_set.clear();
        let mut all = dirty;
        all.append(&mut spilled_dirty);
        all
    }
    pub fn len(&self) -> usize {
        self.pages.len() + self.spilled.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty() && self.spilled.is_empty()
    }
    pub fn stats(&self) -> &CacheMetrics {
        &self.metrics
    }
    pub fn reset_stats(&mut self) {
        self.metrics = CacheMetrics::default();
    }
}
impl Default for PageCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{Page, PAGE_TYPE_LEAF};

    #[test]
    fn retains_dirty_pages_on_eviction() {
        let mut cache = PageCache::with_capacity(1);

        let page_a = Page::new(2, PAGE_TYPE_LEAF);
        cache.insert(page_a, true);

        let page_b = Page::new(3, PAGE_TYPE_LEAF);
        cache.insert(page_b, true);

        assert!(cache.contains(2));
        assert!(cache.contains(3));
        assert!(cache.is_dirty(2));
        assert!(cache.is_dirty(3));

        let a = cache.get(2).unwrap();
        assert_eq!(a.header.page_id, 2);

        cache.clear_dirty();

        assert!(!cache.is_dirty(2));
        assert!(!cache.is_dirty(3));
        assert!(cache.get(2).is_none());
        assert!(cache.get(3).is_some());
    }
}
