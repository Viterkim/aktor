# aktor-macros

Macros for [Aktor](https://docs.rs/aktor/latest/aktor/), you use this through the `aktor` crate.

```rust
#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;

    *count
}
```

You can pass a handle to queue the call, or a state reference to run it right there.

[Aktor README](https://github.com/Viterkim/aktor#readme)
