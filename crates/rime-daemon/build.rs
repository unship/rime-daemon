fn main() {
    // 链接真 librime 只发生在 daemon(核心不变式:shim/client 零 librime)。
    // 放在本 crate 而非 rime-sys 的 feature,避免 workspace feature 统一污染 shim。
    let lib_dir = std::env::var("RIME_LIB_DIR").unwrap_or_else(|_| "/opt/homebrew/lib".into());
    println!("cargo:rustc-link-search=native={lib_dir}");
    println!("cargo:rustc-link-lib=dylib=rime");
    println!("cargo:rerun-if-env-changed=RIME_LIB_DIR");
}
