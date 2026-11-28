use std::env;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(aktor_serve_selected)");

    if env::var("CARGO_CFG_TARGET_FAMILY")
        .is_ok_and(|families| families.split(',').any(|family| family == "wasm"))
        && env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("unknown")
    {
        panic!(
            "aktor does not support non-browser Wasm targets. Use wasm32-unknown-unknown for browser builds."
        );
    }

    // Rust can test a specific cfg value, but only Cargo tells us whether any value was set.
    if env::var_os("CARGO_CFG_AKTOR_SERVE").is_some()
        && env::var_os("CARGO_FEATURE_WASM_BROWSER_WORKER_SERVER").is_some()
    {
        println!("cargo::rustc-cfg=aktor_serve_selected");
    }
}
