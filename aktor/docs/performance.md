# Performance

## Direct state

Calling the annotated function with a state reference:

```text
Handwritten function         5.6 ns
Aktor function              14.2 ns
```

## Native tasks

Empty counter, call and reply:

```text
Handwritten local task       176 ns     1 allocation
Handwritten task             182 ns     1 allocation
Actify                       216 ns     4 allocations
Aktor TokioLocal             257 ns     8 allocations
Aktor TokioTask              324 ns     2 allocations
Kameo explicit send          380 ns     4 allocations
Kameo                        387 ns     5 allocations
```

## Dedicated threads

```text
Handwritten thread         1.487 us
Aktor StdThread            1.989 us
Aktor TokioThread          2.260 us
```

## Browser workers

### 1 MiB bytes

```text
Shared Wasm thread         0.041 ms
AktorData                  0.355 ms
Aktor Serde                0.469 ms
Leptos transferable        1.525 ms
Leptos                     3.225 ms
Yew                        3.256 ms
Gloo                       3.271 ms
```

### 10,485 records

An id and two strings each:

```text
Shared Wasm thread         0.041 ms
AktorData                  1.994 ms
Aktor Serde                3.491 ms
Yew                        3.518 ms
Gloo                       4.388 ms
Leptos                    20.497 ms
```

### 8 bytes

```text
Yew                           12 us
Gloo                          12 us
Leptos transferable           14 us
Leptos                        15 us
AktorData                     36 us
Aktor Serde                   36 us
Shared Wasm thread            40 us
```

## AktorData

CPU time per encode / decode:

```text
                     Bytes       Encode       Decode
4,096 booleans         515      0.35 us       0.43 us
4,096 f64s          32,771      0.46 us       0.43 us
```

## TCP

2 MiB patterned bytes, loopback echo:

```text
CPU time                    2.42 ms
Round trip                  2.46 ms
```
