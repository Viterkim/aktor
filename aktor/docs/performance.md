# Performance

This probably doesn't matter for your app (your SQL query or network call is way slower than a channel send), it's still fun to see what the overhead actually is.

The native timings use one caller on a Core Ultra 9 285H running Linux (caller and owner sharing one core in the round trip benchmarks).

## Direct state

When you already have the state, `step(&mut state, work).await` runs the call on it without allocating.

```text
Handwritten function        2.0 ns     0 allocations
step                       18.6 ns     0 allocations
```

## Native tasks

Each measurement covers one call and reply on an empty counter.

```text
Handwritten local task      187 ns     1 allocation
Handwritten task            191 ns     1 allocation
Aktor TokioLocal            199 ns     3 allocations
Actify                      228 ns     4 allocations
Aktor TokioTask             320 ns     2 allocations
Kameo                       407 ns     5 allocations
Kameo explicit send         411 ns     4 allocations
```

Casting counter increments to a `TokioTask` took 174 ns each, with a queue of 100 that keeps filling up. A cast that finds room needs one allocation.

## Dedicated threads

```text
Handwritten thread         1.62 µs
Aktor TokioThread          1.81 µs
Aktor StdThread            1.82 µs
```

## Browser workers

In Chromium 153, one caller sends owned Rust data to a worker and gets it back. The shared Wasm thread can hand back the very same `Vec` allocation. The other workers have their own heaps, so their times include moving the data between them.

### 1 MiB bytes

```text
Shared Wasm thread        0.040 ms
AktorData                 0.400 ms
Leptos transferable       1.582 ms
Leptos                    3.331 ms
Yew                       3.407 ms
Gloo                      3.436 ms
```

### 10,485 records

Each record has an ID and two strings.

```text
Shared Wasm thread        0.044 ms
AktorData                 1.921 ms
Yew                       3.692 ms
Gloo                      4.423 ms
Leptos                   21.453 ms
```

### 8 bytes

```text
Gloo                         13 µs
Yew                          14 µs
Leptos                       18 µs
Leptos transferable          19 µs
Shared Wasm thread           38 µs
AktorData                    39 µs
```

## AktorData

Encode and decode are CPU time per value.

```text
                     Bytes       Encode       Decode
4,096 booleans         515      0.35 µs     0.40 µs
4,096 signed bytes   4,098      0.07 µs     0.05 µs
4,096 f64s          32,771      0.46 µs     0.42 µs
```

## aktor-remote

An 8 byte echo on loopback took 9.5 µs over TCP and 7.5 µs over a Unix socket. A 2 MiB patterned echo over TCP took 3.7 ms round trip on one core.
