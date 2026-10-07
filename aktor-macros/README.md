# aktor-macros

Use through aktor:

```rust
#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;
    *count
}
```

Pass a handle to queue it, or the state to run it right there.

[Readme](../README.md)
