# aktor-macros

Use this through the `aktor` crate.

```rust
#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;

    *count
}
```

Pass a handle to queue the call or a state reference to run it right there.

[Aktor README](https://github.com/Viterkim/aktor#readme)
