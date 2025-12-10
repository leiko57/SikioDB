use crate::btree::BTreeNode;
use crate::cache::PageCache;
use crate::error::Result;
use crate::page::Page;
use crate::storage::OPFSStorage;
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
    state.clear();
    if root_page_id == 0 {
        return Ok(false);
    }
    let mut current_page_id = root_page_id;
    loop {
        let node = load_node_for_cursor(current_page_id, storage, cache)?;
        if node.is_leaf {
            if node.keys.is_empty() {
                return Ok(false);
            }
            state.stack.push(StackEntry {
                page_id: current_page_id,
                key_index: 0,
            });
            state.current_key = Some(node.keys[0].clone());
            state.current_value = Some(node.values[0].clone());
            return Ok(true);
        } else {
            state.stack.push(StackEntry {
                page_id: current_page_id,
                key_index: 0,
            });
            if node.children.is_empty() {
                return Ok(false);
            }
            current_page_id = node.children[0];
        }
    }
}
pub fn cursor_last(
    state: &mut CursorState,
    root_page_id: u64,
    storage: &OPFSStorage,
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
            if node.keys.is_empty() {
                return Ok(false);
            }
            let last_idx = node.keys.len() - 1;
            state.stack.push(StackEntry {
                page_id: current_page_id,
                key_index: last_idx,
            });
            state.current_key = Some(node.keys[last_idx].clone());
            state.current_value = Some(node.values[last_idx].clone());
            return Ok(true);
        } else {
            let last_child_idx = node.children.len() - 1;
            state.stack.push(StackEntry {
                page_id: current_page_id,
                key_index: node.keys.len(),
            });
            current_page_id = node.children[last_child_idx];
        }
    }
}
pub fn cursor_seek(
    state: &mut CursorState,
    key: &[u8],
    root_page_id: u64,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    state.clear();
    if root_page_id == 0 {
        return Ok(false);
    }
    let mut current_page_id = root_page_id;
    loop {
        let node = load_node_for_cursor(current_page_id, storage, cache)?;
        let pos = node.find_key_position(key);
        if node.is_leaf {
            if pos < node.keys.len() {
                state.stack.push(StackEntry {
                    page_id: current_page_id,
                    key_index: pos,
                });
                state.current_key = Some(node.keys[pos].clone());
                state.current_value = Some(node.values[pos].clone());
                return Ok(true);
            } else {
                state.stack.push(StackEntry {
                    page_id: current_page_id,
                    key_index: pos,
                });
                return cursor_next_internal(state, storage, cache);
            }
        } else {
            state.stack.push(StackEntry {
                page_id: current_page_id,
                key_index: pos,
            });
            if pos < node.children.len() {
                current_page_id = node.children[pos];
            } else {
                return Ok(false);
            }
        }
    }
}
pub fn cursor_next(
    state: &mut CursorState,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    if state.stack.is_empty() {
        return Ok(false);
    }
    let top = state.stack.last_mut().unwrap();
    top.key_index += 1;
    cursor_next_internal(state, storage, cache)
}
fn cursor_next_internal(
    state: &mut CursorState,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    while !state.stack.is_empty() {
        let top = *state.stack.last().unwrap();
        let node = load_node_for_cursor(top.page_id, storage, cache)?;
        if node.is_leaf {
            if top.key_index < node.keys.len() {
                state.current_key = Some(node.keys[top.key_index].clone());
                state.current_value = Some(node.values[top.key_index].clone());
                return Ok(true);
            } else {
                state.stack.pop();
            }
        } else {
            if top.key_index < node.children.len() {
                let child_id = node.children[top.key_index];
                state.stack.push(StackEntry {
                    page_id: child_id,
                    key_index: 0,
                });
            } else {
                state.stack.pop();
            }
        }
    }
    state.current_key = None;
    state.current_value = None;
    Ok(false)
}
pub fn cursor_prev(
    state: &mut CursorState,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    if state.stack.is_empty() {
        return Ok(false);
    }
    while !state.stack.is_empty() {
        let top = state.stack.last_mut().unwrap();
        if top.key_index > 0 {
            top.key_index -= 1;
            let node = load_node_for_cursor(top.page_id, storage, cache)?;
            if node.is_leaf {
                state.current_key = Some(node.keys[top.key_index].clone());
                state.current_value = Some(node.values[top.key_index].clone());
                return Ok(true);
            } else {
                let child_id = node.children[top.key_index + 1];
                return descend_to_last(state, child_id, storage, cache);
            }
        } else {
            state.stack.pop();
            if let Some(parent) = state.stack.last() {
                let parent_node = load_node_for_cursor(parent.page_id, storage, cache)?;
                if !parent_node.is_leaf && parent.key_index > 0 {
                    let key_idx = parent.key_index - 1;
                    if key_idx < parent_node.keys.len() {
                        state.current_key = Some(parent_node.keys[key_idx].clone());
                        state.current_value = None;
                    }
                }
            }
        }
    }
    state.current_key = None;
    state.current_value = None;
    Ok(false)
}
fn descend_to_last(
    state: &mut CursorState,
    mut page_id: u64,
    storage: &OPFSStorage,
    cache: &mut PageCache,
) -> Result<bool> {
    loop {
        let node = load_node_for_cursor(page_id, storage, cache)?;
        if node.is_leaf {
            if node.keys.is_empty() {
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
        } else {
            let last_child_idx = node.children.len() - 1;
            state.stack.push(StackEntry {
                page_id,
                key_index: last_child_idx,
            });
            page_id = node.children[last_child_idx];
        }
    }
}
fn load_node_for_cursor(
    page_id: u64,
    storage: &OPFSStorage,
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
