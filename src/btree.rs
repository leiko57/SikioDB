use crate::error::{Result, SikioError};
use crate::page::{CellPointer, Page, PAGE_DATA_SIZE, PAGE_TYPE_INTERNAL, PAGE_TYPE_LEAF};
#[allow(dead_code)]
const MIN_KEYS_PER_NODE: usize = 2;
const CELL_POINTER_SIZE: usize = CellPointer::SIZE;
const CHILD_POINTER_SIZE: usize = 8;
const SPLIT_THRESHOLD: usize = PAGE_DATA_SIZE - 64;
#[derive(Debug, Clone)]
pub struct BTreeNode {
    pub page_id: u64,
    pub is_leaf: bool,
    pub keys: Vec<Vec<u8>>,
    pub values: Vec<Vec<u8>>,
    pub children: Vec<u64>,
}
impl BTreeNode {
    pub fn new_leaf(page_id: u64) -> Self {
        BTreeNode {
            page_id,
            is_leaf: true,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }
    pub fn new_internal(page_id: u64) -> Self {
        BTreeNode {
            page_id,
            is_leaf: false,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }
    pub fn from_page(page: &Page) -> Result<Self> {
        let is_leaf = page.header.page_type == PAGE_TYPE_LEAF;
        let item_count = page.header.item_count as usize;
        let mut node = if is_leaf {
            let mut n = BTreeNode::new_leaf(page.header.page_id);
            n.keys.reserve(item_count);
            n.values.reserve(item_count);
            n
        } else {
            let mut n = BTreeNode::new_internal(page.header.page_id);
            n.keys.reserve(item_count);
            n.children.reserve(item_count + 1);
            n
        };
        let data = &page.data;
        let mut ptr_offset = 0;
        for i in 0..item_count {
            if ptr_offset + CELL_POINTER_SIZE > data.len() {
                return Err(SikioError::PageCorrupted {
                    page_id: page.header.page_id,
                    reason: "Cell pointer overflow".into(),
                });
            }
            let cell_ptr =
                CellPointer::from_bytes(&data[ptr_offset..ptr_offset + CELL_POINTER_SIZE]);
            ptr_offset += CELL_POINTER_SIZE;
            let cell_start = cell_ptr.offset as usize;
            let cell_len = cell_ptr.length as usize;
            if cell_start + cell_len > PAGE_DATA_SIZE {
                return Err(SikioError::PageCorrupted {
                    page_id: page.header.page_id,
                    reason: "Cell data overflow".into(),
                });
            }
            let cell_data = &data[cell_start..cell_start + cell_len];
            if is_leaf {
                let (key, value) = Self::decode_leaf_cell(cell_data)?;
                node.keys.push(key);
                node.values.push(value);
            } else {
                let (key, child_id) = Self::decode_internal_cell(cell_data)?;
                node.keys.push(key);
                if node.children.len() <= i {
                    node.children.push(child_id);
                }
            }
        }
        if !is_leaf && item_count > 0 {
            let rightmost_offset = ptr_offset;
            if rightmost_offset + CHILD_POINTER_SIZE <= data.len() {
                let rightmost = u64::from_le_bytes(
                    data[rightmost_offset..rightmost_offset + CHILD_POINTER_SIZE]
                        .try_into()
                        .map_err(|_| SikioError::PageCorrupted {
                            page_id: page.header.page_id,
                            reason: "Invalid child pointer".into(),
                        })?,
                );
                if rightmost != 0 {
                    node.children.push(rightmost);
                }
            }
        }
        Ok(node)
    }
    pub fn to_page(&self) -> Result<Page> {
        let page_type = if self.is_leaf {
            PAGE_TYPE_LEAF
        } else {
            PAGE_TYPE_INTERNAL
        };
        let mut page = Page::new(self.page_id, page_type);
        let ptr_area_size = self.keys.len() * CELL_POINTER_SIZE
            + if !self.is_leaf { CHILD_POINTER_SIZE } else { 0 };
        let mut cells: Vec<Vec<u8>> = Vec::with_capacity(self.keys.len());
        let mut total_cells_size = 0usize;
        for i in 0..self.keys.len() {
            let cell = if self.is_leaf {
                Self::encode_leaf_cell(&self.keys[i], &self.values[i])
            } else {
                let child_id = if i < self.children.len() {
                    self.children[i]
                } else {
                    0
                };
                Self::encode_internal_cell(&self.keys[i], child_id)
            };
            total_cells_size += cell.len();
            cells.push(cell);
        }
        if ptr_area_size + total_cells_size > PAGE_DATA_SIZE {
            return Err(SikioError::BTreeOverflow);
        }
        let mut current_offset = PAGE_DATA_SIZE;
        let mut cell_pointers: Vec<CellPointer> = Vec::with_capacity(self.keys.len());
        for cell in &cells {
            current_offset -= cell.len();
            cell_pointers.push(CellPointer {
                offset: current_offset as u16,
                length: cell.len() as u16,
            });
            page.data[current_offset..current_offset + cell.len()].copy_from_slice(cell);
        }
        let mut write_offset = 0;
        for ptr in &cell_pointers {
            let ptr_bytes = ptr.to_bytes();
            page.data[write_offset..write_offset + CELL_POINTER_SIZE].copy_from_slice(&ptr_bytes);
            write_offset += CELL_POINTER_SIZE;
        }
        if !self.is_leaf && !self.children.is_empty() {
            let rightmost = self.children.last().copied().unwrap_or(0);
            page.data[write_offset..write_offset + CHILD_POINTER_SIZE]
                .copy_from_slice(&rightmost.to_le_bytes());
        }
        page.header.item_count = self.keys.len() as u16;
        page.header.free_space_offset = current_offset as u16;
        Ok(page)
    }
    fn encode_leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut cell = Vec::with_capacity(4 + key.len() + value.len());
        cell.extend_from_slice(&(key.len() as u16).to_le_bytes());
        cell.extend_from_slice(&(value.len() as u16).to_le_bytes());
        cell.extend_from_slice(key);
        cell.extend_from_slice(value);
        cell
    }
    fn decode_leaf_cell(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        if data.len() < 4 {
            return Err(SikioError::PageCorrupted {
                page_id: 0,
                reason: "Leaf cell too short".into(),
            });
        }
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let value_len = u16::from_le_bytes([data[2], data[3]]) as usize;
        if data.len() < 4 + key_len + value_len {
            return Err(SikioError::PageCorrupted {
                page_id: 0,
                reason: "Leaf cell data truncated".into(),
            });
        }
        let key = data[4..4 + key_len].to_vec();
        let value = data[4 + key_len..4 + key_len + value_len].to_vec();
        Ok((key, value))
    }
    fn encode_internal_cell(key: &[u8], child_id: u64) -> Vec<u8> {
        let mut cell = Vec::with_capacity(2 + CHILD_POINTER_SIZE + key.len());
        cell.extend_from_slice(&(key.len() as u16).to_le_bytes());
        cell.extend_from_slice(&child_id.to_le_bytes());
        cell.extend_from_slice(key);
        cell
    }
    fn decode_internal_cell(data: &[u8]) -> Result<(Vec<u8>, u64)> {
        if data.len() < 2 + CHILD_POINTER_SIZE {
            return Err(SikioError::PageCorrupted {
                page_id: 0,
                reason: "Internal cell too short".into(),
            });
        }
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let child_id =
            u64::from_le_bytes(
                data[2..10]
                    .try_into()
                    .map_err(|_| SikioError::PageCorrupted {
                        page_id: 0,
                        reason: "Invalid child pointer bytes".into(),
                    })?,
            );
        if data.len() < 2 + CHILD_POINTER_SIZE + key_len {
            return Err(SikioError::PageCorrupted {
                page_id: 0,
                reason: "Internal cell data truncated".into(),
            });
        }
        let key = data[2 + CHILD_POINTER_SIZE..2 + CHILD_POINTER_SIZE + key_len].to_vec();
        Ok((key, child_id))
    }
    pub fn find_key_position(&self, key: &[u8]) -> usize {
        self.keys
            .binary_search_by(|k| k.as_slice().cmp(key))
            .unwrap_or_else(|i| i)
    }
    pub fn insert_at(&mut self, pos: usize, key: Vec<u8>, value: Vec<u8>) {
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
    }
    pub fn insert_internal(&mut self, pos: usize, key: Vec<u8>, right_child: u64) {
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, right_child);
    }
    pub fn estimated_size(&self) -> usize {
        let ptr_size = self.keys.len() * CELL_POINTER_SIZE;
        let rightmost_size = if self.is_leaf { 0 } else { CHILD_POINTER_SIZE };
        let cells_size: usize = if self.is_leaf {
            self.keys
                .iter()
                .zip(&self.values)
                .map(|(k, v)| 4 + k.len() + v.len())
                .sum()
        } else {
            self.keys
                .iter()
                .map(|k| 2 + CHILD_POINTER_SIZE + k.len())
                .sum()
        };
        ptr_size + rightmost_size + cells_size
    }
    pub fn needs_split(&self) -> bool {
        self.estimated_size() > SPLIT_THRESHOLD
    }
    pub fn split(&mut self) -> (Vec<u8>, BTreeNode) {
        let mid = self.keys.len() / 2;
        if self.is_leaf {
            let mut right = BTreeNode::new_leaf(0);
            right.keys = self.keys.split_off(mid);
            right.values = self.values.split_off(mid);
            let separator = right.keys[0].clone();
            (separator, right)
        } else {
            let separator = self.keys.remove(mid);
            let mut right = BTreeNode::new_internal(0);
            right.keys = self.keys.split_off(mid);
            right.children = self.children.split_off(mid + 1);
            (separator, right)
        }
    }
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    pub fn min_keys() -> usize {
        MIN_KEYS_PER_NODE
    }
    pub fn is_underflow(&self) -> bool {
        self.keys.len() < MIN_KEYS_PER_NODE
    }
    pub fn can_lend(&self) -> bool {
        self.keys.len() > MIN_KEYS_PER_NODE
    }
    pub fn borrow_from_left(
        &mut self,
        left: &mut BTreeNode,
        parent_key: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if self.is_leaf {
            let borrowed_key = left.keys.pop().ok_or(SikioError::PageCorrupted {
                page_id: left.page_id,
                reason: "Left sibling empty".into(),
            })?;
            let borrowed_val = left.values.pop().ok_or(SikioError::PageCorrupted {
                page_id: left.page_id,
                reason: "Left sibling empty vals".into(),
            })?;
            self.keys.insert(0, borrowed_key);
            self.values.insert(0, borrowed_val);
            Ok(self.keys[0].clone())
        } else {
            let borrowed_key = left.keys.pop().ok_or(SikioError::PageCorrupted {
                page_id: left.page_id,
                reason: "Left sibling empty keys".into(),
            })?;
            let borrowed_child = left.children.pop().ok_or(SikioError::PageCorrupted {
                page_id: left.page_id,
                reason: "Left sibling empty children".into(),
            })?;
            self.keys.insert(0, parent_key);
            self.children.insert(0, borrowed_child);
            Ok(borrowed_key)
        }
    }
    pub fn borrow_from_right(
        &mut self,
        right: &mut BTreeNode,
        parent_key: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if right.keys.is_empty() {
            return Err(SikioError::PageCorrupted {
                page_id: right.page_id,
                reason: "Right sibling empty".into(),
            });
        }
        if self.is_leaf {
            let borrowed_key = right.keys.remove(0);
            let borrowed_val = right.values.remove(0);
            self.keys.push(borrowed_key);
            self.values.push(borrowed_val);
            Ok(right
                .keys
                .get(0)
                .ok_or(SikioError::PageCorrupted {
                    page_id: right.page_id,
                    reason: "Right keys exhausted".into(),
                })?
                .clone())
        } else {
            let borrowed_key = right.keys.remove(0);
            let borrowed_child = right.children.remove(0);
            self.keys.push(parent_key);
            self.children.push(borrowed_child);
            Ok(borrowed_key)
        }
    }
    pub fn merge_with(&mut self, right: BTreeNode, separator: Vec<u8>) {
        if self.is_leaf {
            self.keys.extend(right.keys);
            self.values.extend(right.values);
        } else {
            self.keys.push(separator);
            self.keys.extend(right.keys);
            self.children.extend(right.children);
        }
    }
}
pub struct BTree {
    root_page_id: u64,
    next_page_id: u64,
    free_page_ids: Vec<u64>,
}
impl BTree {
    pub fn new() -> Self {
        BTree {
            root_page_id: 0,
            next_page_id: 2,
            free_page_ids: Vec::new(),
        }
    }
    pub fn with_root(root_id: u64, next_id: u64) -> Self {
        BTree {
            root_page_id: root_id,
            next_page_id: next_id,
            free_page_ids: Vec::new(),
        }
    }
    pub fn root_page_id(&self) -> u64 {
        self.root_page_id
    }
    pub fn set_root(&mut self, page_id: u64) {
        self.root_page_id = page_id;
    }
    pub fn allocate_page(&mut self) -> u64 {
        if let Some(id) = self.free_page_ids.pop() {
            return id;
        }
        let id = self.next_page_id;
        self.next_page_id += 1;
        id
    }
    pub fn reclaim_page(&mut self, page_id: u64) {
        let pos = self
            .free_page_ids
            .binary_search(&page_id)
            .unwrap_or_else(|i| i);
        self.free_page_ids.insert(pos, page_id);
    }
    pub fn next_page_id(&self) -> u64 {
        self.next_page_id
    }
    pub fn set_next_page_id(&mut self, id: u64) {
        self.next_page_id = id;
    }
    pub fn free_page_ids(&self) -> &[u64] {
        &self.free_page_ids
    }
    pub fn set_free_page_ids(&mut self, ids: Vec<u64>) {
        self.free_page_ids = ids;
    }
}
impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}
