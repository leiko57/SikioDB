use crate::btree::BTreeNode;
use crate::cache::PageCache;
use crate::error::Result;
use crate::page::Page;
use crate::storage::OPFSStorage;

trait CursorStorage {
    fn read_page(&self, page_id: u64) -> Result<Vec<u8>>;
}

impl CursorStorage for OPFSStorage {
    fn read_page(&self, page_id: u64) -> Result<Vec<u8>> {
        OPFSStorage::read_page(self, page_id)
    }
}
#[derive(Debug, Clone, Copy)]
struct StackEntry {
    page_id: u64,
    key_index: usize,
}
pub struct CursorState {
    stack: Vec<StackEntry>,
    current_key: Option<Vec<u8>>,
    current_value: Option<Vec<u8>>,
}
impl CursorState {
    pub fn new() -> Self {
        CursorState {
            stack: Vec::with_capacity(16),
            current_key: None,
            current_value: None,
        }
    }
    pub fn key(&self) -> Option<&[u8]> {
        self.current_key.as_deref()
    }
    pub fn value(&self) -> Option<&[u8]> {
        self.current_value.as_deref()
    }
    pub fn valid(&self) -> bool {
        self.current_key.is_some()
    }
    pub fn clear(&mut self) {
        self.stack.clear();
        self.current_key = None;
        self.current_value = None;
    }
}
pub fn cursor_first(
    state: &mut CursorState,
    root_page_id: u64,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    cursor_first_internal(state, root_page_id, storage, cache)
}
pub fn cursor_last(
    state: &mut CursorState,
    root_page_id: u64,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    cursor_last_internal(state, root_page_id, storage, cache)
}
pub fn cursor_seek(
    state: &mut CursorState,
    key: &[u8],
    root_page_id: u64,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    cursor_seek_internal(state, key, root_page_id, storage, cache)
}
pub fn cursor_next(
    state: &mut CursorState,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    cursor_next_internal(state, storage, cache, true)
}
pub fn cursor_prev(
    state: &mut CursorState,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    cursor_prev_internal(state, storage, cache)
}

fn cursor_first_internal<S: CursorStorage>(
    state: &mut CursorState,
    root_page_id: u64,
    storage: &S,
    cache: &mut PageCache,
) -> Result<bool> {
    state.clear();
    if root_page_id == 0 {
        return Ok(false);
    }
    descend_leftmost(state, root_page_id, storage, cache)
}

fn cursor_last_internal<S: CursorStorage>(
    state: &mut CursorState,
    root_page_id: u64,
    storage: &S,
    cache: &mut PageCache,
) -> Result<bool> {
    state.clear();
    if root_page_id == 0 {
        return Ok(false);
    }
    descend_rightmost(state, root_page_id, storage, cache)
}

fn cursor_seek_internal<S: CursorStorage>(
    state: &mut CursorState,
    key: &[u8],
    root_page_id: u64,
    storage: &S,
    cache: &mut PageCache,
) -> Result<bool> {
    state.clear();
    if root_page_id == 0 {
        return Ok(false);
    }

    let mut current_page_id = root_page_id;
    loop {
        let node = load_node_for_cursor(current_page_id, storage, cache)?;
        if node.is_leaf {
            let pos = node.find_key_position(key);
            state.stack.push(StackEntry {
                page_id: current_page_id,
                key_index: pos,
            });
            if pos < node.keys.len() {
                state.current_key = Some(node.keys[pos].clone());
                state.current_value = Some(node.values[pos].clone());
                return Ok(true);
            }
            state.current_key = None;
            state.current_value = None;
            return cursor_next_internal(state, storage, cache, false);
        }

        if node.children.is_empty() {
            state.current_key = None;
            state.current_value = None;
            return Ok(false);
        }

        let pos = node.find_key_position(key);
        let child_idx = if pos < node.keys.len() && node.keys[pos].as_slice() == key {
            pos + 1
        } else {
            pos
        };

        state.stack.push(StackEntry {
            page_id: current_page_id,
            key_index: child_idx,
        });

        if child_idx >= node.children.len() {
            state.current_key = None;
            state.current_value = None;
            return Ok(false);
        }

        current_page_id = node.children[child_idx];
    }
}

fn cursor_next_internal<S: CursorStorage>(
    state: &mut CursorState,
    storage: &S,
    cache: &mut PageCache,
    increment_leaf: bool,
) -> Result<bool> {
    if state.stack.is_empty() {
        state.current_key = None;
        state.current_value = None;
        return Ok(false);
    }

    if increment_leaf {
        if let Some(leaf_entry) = state.stack.last_mut() {
            leaf_entry.key_index = leaf_entry.key_index.saturating_add(1);
        }
    }

    loop {
        let leaf_entry = match state.stack.last().copied() {
            Some(e) => e,
            None => {
                state.current_key = None;
                state.current_value = None;
                return Ok(false);
            }
        };

        let leaf = load_node_for_cursor(leaf_entry.page_id, storage, cache)?;
        if !leaf.is_leaf {
            state.stack.pop();
            continue;
        }

        if leaf_entry.key_index < leaf.keys.len() {
            state.current_key = Some(leaf.keys[leaf_entry.key_index].clone());
            state.current_value = Some(leaf.values[leaf_entry.key_index].clone());
            return Ok(true);
        }

        state.stack.pop();

        while let Some(parent_entry) = state.stack.last_mut() {
            let parent = load_node_for_cursor(parent_entry.page_id, storage, cache)?;
            if parent.is_leaf {
                state.stack.pop();
                continue;
            }

            let next_child_idx = parent_entry.key_index + 1;
            if next_child_idx < parent.children.len() {
                parent_entry.key_index = next_child_idx;
                let child_id = parent.children[next_child_idx];
                return descend_leftmost(state, child_id, storage, cache);
            }

            state.stack.pop();
        }

        state.current_key = None;
        state.current_value = None;
        return Ok(false);
    }
}

fn cursor_prev_internal<S: CursorStorage>(
    state: &mut CursorState,
    storage: &S,
    cache: &mut PageCache,
) -> Result<bool> {
    if state.stack.is_empty() {
        state.current_key = None;
        state.current_value = None;
        return Ok(false);
    }

    loop {
        let leaf_entry = match state.stack.last_mut() {
            Some(e) => e,
            None => {
                state.current_key = None;
                state.current_value = None;
                return Ok(false);
            }
        };

        let leaf = load_node_for_cursor(leaf_entry.page_id, storage, cache)?;
        if !leaf.is_leaf {
            state.stack.pop();
            continue;
        }

        if leaf_entry.key_index > 0 && leaf_entry.key_index <= leaf.keys.len() {
            leaf_entry.key_index -= 1;
            state.current_key = Some(leaf.keys[leaf_entry.key_index].clone());
            state.current_value = Some(leaf.values[leaf_entry.key_index].clone());
            return Ok(true);
        }

        state.stack.pop();

        while let Some(parent_entry) = state.stack.last_mut() {
            let parent = load_node_for_cursor(parent_entry.page_id, storage, cache)?;
            if parent.is_leaf {
                state.stack.pop();
                continue;
            }

            if parent_entry.key_index > 0 {
                parent_entry.key_index -= 1;
                let child_id = parent.children[parent_entry.key_index];
                return descend_rightmost(state, child_id, storage, cache);
            }

            state.stack.pop();
        }

        state.current_key = None;
        state.current_value = None;
        return Ok(false);
    }
}

fn descend_leftmost<S: CursorStorage>(
    state: &mut CursorState,
    mut page_id: u64,
    storage: &S,
    cache: &mut PageCache,
) -> Result<bool> {
    loop {
        let node = load_node_for_cursor(page_id, storage, cache)?;
        if node.is_leaf {
            if node.keys.is_empty() {
                state.current_key = None;
                state.current_value = None;
                return Ok(false);
            }
            state.stack.push(StackEntry {
                page_id,
                key_index: 0,
            });
            state.current_key = Some(node.keys[0].clone());
            state.current_value = Some(node.values[0].clone());
            return Ok(true);
        }

        if node.children.is_empty() {
            state.current_key = None;
            state.current_value = None;
            return Ok(false);
        }

        state.stack.push(StackEntry {
            page_id,
            key_index: 0,
        });
        page_id = node.children[0];
    }
}

fn descend_rightmost<S: CursorStorage>(
    state: &mut CursorState,
    mut page_id: u64,
    storage: &S,
    cache: &mut PageCache,
) -> Result<bool> {
    loop {
        let node = load_node_for_cursor(page_id, storage, cache)?;
        if node.is_leaf {
            if node.keys.is_empty() {
                state.current_key = None;
                state.current_value = None;
                return Ok(false);
            }
            let last_idx = node.keys.len() - 1;
            state.stack.push(StackEntry {
                page_id,
                key_index: last_idx,
            });
            state.current_key = Some(node.keys[last_idx].clone());
            state.current_value = Some(node.values[last_idx].clone());
            return Ok(true);
        }

        if node.children.is_empty() {
            state.current_key = None;
            state.current_value = None;
            return Ok(false);
        }

        let last_child_idx = node.children.len() - 1;
        state.stack.push(StackEntry {
            page_id,
            key_index: last_child_idx,
        });
        page_id = node.children[last_child_idx];
    }
}

fn load_node_for_cursor<S: CursorStorage>(
    page_id: u64,
    storage: &S,
    cache: &mut PageCache,
) -> Result<BTreeNode> {
    if let Some(page) = cache.get(page_id) {
        return BTreeNode::from_page(page);
    }
    let bytes = storage.read_page(page_id)?;
    let page = Page::from_bytes(&bytes)?;
    let node = BTreeNode::from_page(&page)?;
    cache.insert(page, false);
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::BTreeNode;
    use crate::cache::PageCache;
    use crate::error::SikioError;
    use std::collections::HashMap;

    struct MemoryStorage {
        pages: HashMap<u64, Vec<u8>>,
    }

    impl MemoryStorage {
        fn new() -> Self {
            MemoryStorage {
                pages: HashMap::new(),
            }
        }

        fn put_node(&mut self, node: &BTreeNode) {
            let page = node.to_page().unwrap();
            self.pages.insert(node.page_id, page.to_bytes());
        }
    }

    impl CursorStorage for MemoryStorage {
        fn read_page(&self, page_id: u64) -> Result<Vec<u8>> {
            self.pages.get(&page_id).cloned().ok_or_else(|| {
                SikioError::IoError(format!("Missing page {}", page_id))
            })
        }
    }

    fn bytes(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn cursor_iterates_across_internal_and_leaf_nodes() {
        let mut storage = MemoryStorage::new();
        let mut cache = PageCache::with_capacity(16);

        let mut leaf1 = BTreeNode::new_leaf(3);
        leaf1.keys = vec![bytes("a"), bytes("b")];
        leaf1.values = vec![bytes("va"), bytes("vb")];
        storage.put_node(&leaf1);

        let mut leaf2 = BTreeNode::new_leaf(4);
        leaf2.keys = vec![bytes("c"), bytes("d")];
        leaf2.values = vec![bytes("vc"), bytes("vd")];
        storage.put_node(&leaf2);

        let mut root = BTreeNode::new_internal(2);
        root.keys = vec![bytes("c")];
        root.children = vec![3, 4];
        storage.put_node(&root);

        let mut state = CursorState::new();
        assert!(cursor_first_internal(&mut state, 2, &storage, &mut cache).unwrap());

        let mut seen = Vec::new();
        loop {
            seen.push(String::from_utf8(state.key().unwrap().to_vec()).unwrap());
            if !cursor_next_internal(&mut state, &storage, &mut cache, true).unwrap() {
                break;
            }
        }

        assert_eq!(seen, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn cursor_seek_uses_right_child_on_separator_match() {
        let mut storage = MemoryStorage::new();
        let mut cache = PageCache::with_capacity(16);

        let mut leaf1 = BTreeNode::new_leaf(3);
        leaf1.keys = vec![bytes("a"), bytes("b")];
        leaf1.values = vec![bytes("va"), bytes("vb")];
        storage.put_node(&leaf1);

        let mut leaf2 = BTreeNode::new_leaf(4);
        leaf2.keys = vec![bytes("c"), bytes("d")];
        leaf2.values = vec![bytes("vc"), bytes("vd")];
        storage.put_node(&leaf2);

        let mut root = BTreeNode::new_internal(2);
        root.keys = vec![bytes("c")];
        root.children = vec![3, 4];
        storage.put_node(&root);

        let mut state = CursorState::new();
        assert!(cursor_seek_internal(&mut state, b"c", 2, &storage, &mut cache).unwrap());
        assert_eq!(state.key().unwrap(), b"c");
    }

    #[test]
    fn cursor_last_and_prev_work() {
        let mut storage = MemoryStorage::new();
        let mut cache = PageCache::with_capacity(16);

        let mut leaf1 = BTreeNode::new_leaf(3);
        leaf1.keys = vec![bytes("a"), bytes("b")];
        leaf1.values = vec![bytes("va"), bytes("vb")];
        storage.put_node(&leaf1);

        let mut leaf2 = BTreeNode::new_leaf(4);
        leaf2.keys = vec![bytes("c"), bytes("d")];
        leaf2.values = vec![bytes("vc"), bytes("vd")];
        storage.put_node(&leaf2);

        let mut root = BTreeNode::new_internal(2);
        root.keys = vec![bytes("c")];
        root.children = vec![3, 4];
        storage.put_node(&root);

        let mut state = CursorState::new();
        assert!(cursor_last_internal(&mut state, 2, &storage, &mut cache).unwrap());
        assert_eq!(state.key().unwrap(), b"d");

        assert!(cursor_prev_internal(&mut state, &storage, &mut cache).unwrap());
        assert_eq!(state.key().unwrap(), b"c");

        assert!(cursor_prev_internal(&mut state, &storage, &mut cache).unwrap());
        assert_eq!(state.key().unwrap(), b"b");

        assert!(cursor_prev_internal(&mut state, &storage, &mut cache).unwrap());
        assert_eq!(state.key().unwrap(), b"a");

        assert!(!cursor_prev_internal(&mut state, &storage, &mut cache).unwrap());
        assert!(!state.valid());
    }

    #[test]
    fn cursor_seek_past_end_returns_false() {
        let mut storage = MemoryStorage::new();
        let mut cache = PageCache::with_capacity(16);

        let mut leaf = BTreeNode::new_leaf(2);
        leaf.keys = vec![bytes("a"), bytes("b")];
        leaf.values = vec![bytes("va"), bytes("vb")];
        storage.put_node(&leaf);

        let mut state = CursorState::new();
        assert!(!cursor_seek_internal(&mut state, b"z", 2, &storage, &mut cache).unwrap());
        assert!(!state.valid());
    }
}
