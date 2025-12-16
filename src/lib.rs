pub mod btree;
pub mod cache;
pub mod compaction;
pub mod compression;
pub mod cursor;
pub mod db;

pub mod error;
pub mod index;
pub mod page;
pub mod range;
pub mod readonly;
pub mod schema;
pub mod snapshot;
pub mod stats;
pub mod storage;
pub mod sync;
pub mod transaction;
pub mod wal;
use wasm_bindgen::prelude::*;
#[cfg(feature = "console_error_panic_hook")]
pub fn set_panic_hook() {
    console_error_panic_hook::set_once();
}
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}
#[allow(unused_macros)]
macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}
#[allow(unused_imports)]
pub(crate) use console_log;
#[cfg(not(test))]
#[wasm_bindgen(start)]
pub fn main() -> Result<(), JsValue> {
    #[cfg(feature = "console_error_panic_hook")]
    set_panic_hook();
    Ok(())
}
