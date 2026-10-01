#[cfg(any(feature = "tokio", feature = "embassy"))]
pub fn stopped() -> ! {
    panic!("actor stopped before replying")
}

#[cfg(any(feature = "tokio", feature = "embassy"))]
pub fn consumed() -> ! {
    panic!("actor request output already consumed")
}
