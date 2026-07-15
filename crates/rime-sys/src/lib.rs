//! C++ librime 的 FFI 绑定(bindgen 自 rime_api.h 生成)。
//! feature `link` 打开时链接 librime.dylib —— 只允许 daemon 打开;
//! shim 仅使用这里的类型定义以保证与原库二进制布局一致。
#![allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    dead_code
)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/// rime_api.h 里 Bool 是宏 `#define Bool int`,bindgen 展开后无此类型,这里补一个别名
pub type Bool = ::libc::c_int;
pub const RIME_TRUE: Bool = 1;
pub const RIME_FALSE: Bool = 0;

/// RIME_STRUCT_INIT 的 Rust 等价:data_size = sizeof(Type) - sizeof(int)
#[macro_export]
macro_rules! rime_struct {
    ($ty:ty) => {{
        let mut v: $ty = unsafe { std::mem::zeroed() };
        v.data_size =
            (std::mem::size_of::<$ty>() - std::mem::size_of::<::libc::c_int>()) as ::libc::c_int;
        v
    }};
}
