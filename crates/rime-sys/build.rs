use std::env;
use std::path::PathBuf;

fn main() {
    let include_dir =
        env::var("RIME_INCLUDE_DIR").unwrap_or_else(|_| "/opt/homebrew/include".into());
    let header = format!("{include_dir}/rime_api.h");

    let bindings = bindgen::Builder::default()
        .header(&header)
        .allowlist_function("rime_get_api")
        .allowlist_type("Rime.*")
        .allowlist_var("RIME_.*")
        .derive_debug(false)
        .layout_tests(false)
        .generate()
        .expect("bindgen rime_api.h failed");

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out.join("bindings.rs"))
        .expect("write bindings.rs");

    // 注意:这里绝不发出链接指令。链接 librime 是 rime-daemon 自己 build.rs 的职责 ——
    // 若做成 rime-sys 的 feature,cargo 整仓构建会把 feature 统一到 shim 上,
    // 导致 shim(必须零 librime)也链上真 librime(实测踩过)。
    println!("cargo:rerun-if-changed={header}");
    println!("cargo:rerun-if-env-changed=RIME_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=RIME_LIB_DIR");
}
