# Performance

The native timings use one caller on a Core Ultra 9 285H running Linux. The caller and owner share one core in the round trip benchmarks.

## Direct state

When you already have the state, `step::direct(&mut state, work).await` skips the request and its boxed future. The usual `step(&mut state, work).await` goes through the same request API you use with an actor handle.

```text
Handwritten function        6.0 ns     0 allocations
step::direct                7.2 ns     0 allocations
step                       15.6 ns     1 allocation
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

With `TokioTask`, casting a counter increment took 174 ns per call over 500,000 increments with a queue capacity of 100. That includes draining the queue and reading the final count. A cast with space in the queue needs one allocation.

## Dedicated threads

```text
Handwritten thread         1.78 µs
Aktor TokioThread          2.09 µs
Aktor StdThread            2.10 µs
```

## Browser workers

In Chromium 153, one caller sends owned Rust data to a worker and gets it back. The shared Wasm thread can hand back the very same `Vec` allocation. The other workers have their own heaps, so their times include moving the data between them.

### 1 MiB bytes

```text
Shared Wasm thread        0.040 ms
AktorData                 0.400 ms
Aktor Serde               0.491 ms
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
Aktor Serde               3.610 ms
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
Aktor Serde                  40 µs
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

An eight-byte echo on loopback took 9.5 µs over TCP and 7.5 µs over a Unix socket. With the TCP client and server on one core, writing the length and body together saved about 3 µs per call.

A 2 MiB patterned TCP echo on one core took a median of 3.68 ms round trip and 3.63 ms of CPU after warmup.
