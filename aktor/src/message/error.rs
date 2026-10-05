#[cfg(any(
    any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ),
    feature = "local"
))]
pub fn stopped() -> ! {
    panic!("actor stopped before replying")
}

pub fn consumed() -> ! {
    panic!("actor request output already consumed")
}
