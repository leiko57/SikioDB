use crate::error::{Result, SikioError};
use crate::page::PAGE_SIZE;
use js_sys::{Function, Object, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{FileSystemDirectoryHandle, FileSystemFileHandle};
pub struct OPFSStorage {
    data_handle: JsValue,
    wal_handle: JsValue,
    data_size: u64,
    wal_size: u64,
}
impl OPFSStorage {
    pub async fn open(db_name: &str) -> std::result::Result<Self, JsValue> {
        let root = get_opfs_root().await?;
        let db_dir = get_or_create_directory(&root, db_name).await?;
        let data_file = get_or_create_file(&db_dir, "data.sdb").await?;
        let wal_file = get_or_create_file(&db_dir, "wal.sdb").await?;
        let data_handle = create_sync_handle(&data_file).await?;
        let wal_handle = create_sync_handle(&wal_file).await?;
        let data_size = call_method_number(&data_handle, "getSize", &[])? as u64;
        let wal_size = call_method_number(&wal_handle, "getSize", &[])? as u64;
        Ok(OPFSStorage {
            data_handle,
            wal_handle,
            data_size,
            wal_size,
        })
    }
    pub fn read_page(&self, page_id: u64) -> Result<Vec<u8>> {
        let offset = page_id * PAGE_SIZE as u64;
        if offset + PAGE_SIZE as u64 > self.data_size {
            return Err(SikioError::IoError(format!(
                "Page {} beyond file size",
                page_id
            )));
        }
        let array = Uint8Array::new_with_length(PAGE_SIZE as u32);
        let options = create_at_options(offset);
        call_method(&self.data_handle, "read", &[array.clone().into(), options])
            .map_err(|e| SikioError::IoError(format!("{:?}", e)))?;
        let mut buffer = vec![0u8; PAGE_SIZE];
        array.copy_to(&mut buffer);
        Ok(buffer)
    }
    pub fn write_page(&mut self, page_id: u64, data: &[u8]) -> Result<()> {
        if data.len() != PAGE_SIZE {
            return Err(SikioError::IoError(format!(
                "Invalid page size: {}",
                data.len()
            )));
        }
        let offset = page_id * PAGE_SIZE as u64;
        let array = Uint8Array::from(data);
        let options = create_at_options(offset);
        call_method(&self.data_handle, "write", &[array.into(), options])
            .map_err(|e| SikioError::IoError(format!("{:?}", e)))?;
        let new_end = offset + PAGE_SIZE as u64;
        if new_end > self.data_size {
            self.data_size = new_end;
        }
        Ok(())
    }
    pub fn append_wal(&mut self, data: &[u8]) -> Result<u64> {
        let offset = self.wal_size;
        let array = Uint8Array::from(data);
        let options = create_at_options(offset);
        call_method(&self.wal_handle, "write", &[array.into(), options])
            .map_err(|e| SikioError::IoError(format!("{:?}", e)))?;
        self.wal_size += data.len() as u64;
        Ok(offset)
    }
    pub fn read_wal(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        if offset + length as u64 > self.wal_size {
            return Err(SikioError::IoError("WAL read beyond size".into()));
        }
        let array = Uint8Array::new_with_length(length as u32);
        let options = create_at_options(offset);
        call_method(&self.wal_handle, "read", &[array.clone().into(), options])
            .map_err(|e| SikioError::IoError(format!("{:?}", e)))?;
        let mut buffer = vec![0u8; length];
        array.copy_to(&mut buffer);
        Ok(buffer)
    }
    pub fn flush_data(&self) -> Result<()> {
        call_method(&self.data_handle, "flush", &[])
            .map_err(|e| SikioError::IoError(format!("{:?}", e)))?;
        Ok(())
    }
    pub fn flush_wal(&self) -> Result<()> {
        call_method(&self.wal_handle, "flush", &[])
            .map_err(|e| SikioError::IoError(format!("{:?}", e)))?;
        Ok(())
    }
    pub fn truncate_wal(&mut self) -> Result<()> {
        call_method(&self.wal_handle, "truncate", &[JsValue::from_f64(0.0)])
            .map_err(|e| SikioError::IoError(format!("{:?}", e)))?;
        self.wal_size = 0;
        Ok(())
    }
    pub fn data_page_count(&self) -> u64 {
        self.data_size / PAGE_SIZE as u64
    }
    pub fn wal_size(&self) -> u64 {
        self.wal_size
    }
    pub fn close(self) {
        let _ = call_method(&self.data_handle, "close", &[]);
        let _ = call_method(&self.wal_handle, "close", &[]);
    }
}
fn create_at_options(offset: u64) -> JsValue {
    let options = Object::new();
    let _ = Reflect::set(&options, &"at".into(), &JsValue::from_f64(offset as f64));
    options.into()
}
fn call_method(
    obj: &JsValue,
    method: &str,
    args: &[JsValue],
) -> std::result::Result<JsValue, JsValue> {
    let func = Reflect::get(obj, &method.into())?;
    let func: Function = func.dyn_into()?;
    let args_array = js_sys::Array::new();
    for arg in args {
        args_array.push(arg);
    }
    Reflect::apply(&func, obj, &args_array)
}
fn call_method_number(
    obj: &JsValue,
    method: &str,
    args: &[JsValue],
) -> std::result::Result<f64, JsValue> {
    let result = call_method(obj, method, args)?;
    result
        .as_f64()
        .ok_or_else(|| JsValue::from_str("Expected number"))
}
async fn get_opfs_root() -> std::result::Result<FileSystemDirectoryHandle, JsValue> {
    let global = js_sys::global();
    let navigator = Reflect::get(&global, &"navigator".into())?;
    let storage = Reflect::get(&navigator, &"storage".into())?;
    let get_directory = Reflect::get(&storage, &"getDirectory".into())?;
    let func: Function = get_directory.dyn_into()?;
    let promise = Reflect::apply(&func, &storage, &js_sys::Array::new())?;
    let result = JsFuture::from(js_sys::Promise::from(promise)).await?;
    Ok(result.unchecked_into())
}
async fn get_or_create_directory(
    parent: &FileSystemDirectoryHandle,
    name: &str,
) -> std::result::Result<FileSystemDirectoryHandle, JsValue> {
    let options = Object::new();
    Reflect::set(&options, &"create".into(), &JsValue::TRUE)?;
    let func = Reflect::get(parent, &"getDirectoryHandle".into())?;
    let func: Function = func.dyn_into()?;
    let args = js_sys::Array::new();
    args.push(&JsValue::from_str(name));
    args.push(&options.into());
    let promise = Reflect::apply(&func, parent, &args)?;
    let result = JsFuture::from(js_sys::Promise::from(promise)).await?;
    Ok(result.unchecked_into())
}
async fn get_or_create_file(
    parent: &FileSystemDirectoryHandle,
    name: &str,
) -> std::result::Result<FileSystemFileHandle, JsValue> {
    let options = Object::new();
    Reflect::set(&options, &"create".into(), &JsValue::TRUE)?;
    let func = Reflect::get(parent, &"getFileHandle".into())?;
    let func: Function = func.dyn_into()?;
    let args = js_sys::Array::new();
    args.push(&JsValue::from_str(name));
    args.push(&options.into());
    let promise = Reflect::apply(&func, parent, &args)?;
    let result = JsFuture::from(js_sys::Promise::from(promise)).await?;
    Ok(result.unchecked_into())
}
async fn create_sync_handle(file: &FileSystemFileHandle) -> std::result::Result<JsValue, JsValue> {
    let func = Reflect::get(file, &"createSyncAccessHandle".into())?;
    let func: Function = func.dyn_into()?;
    let promise = Reflect::apply(&func, file, &js_sys::Array::new())?;
    let result = JsFuture::from(js_sys::Promise::from(promise)).await?;
    Ok(result)
}
