# Performance

## Native tasks

Empty counter, call and reply:

```text
Handwritten task       181 ns     1 allocation
Actify                 211 ns     4 allocations
Aktor TokioTask        320 ns     2 allocations
Kameo                  383 ns     5 allocations
Kameo explicit send    375 ns     4 allocations
```

## Dedicated threads

```text
Handwritten thread    1.517 us
Aktor StdThread       2.001 us
Aktor TokioThread     2.361 us
```

## Browser workers

### 8 bytes

```text
Shared Wasm thread      37 us
AktorData               41 us
Gloo                    13 us
Yew                     13 us
Leptos                  17 us
```

### 1 MiB bytes

```text
Shared Wasm thread    0.039 ms
AktorData             0.422 ms
Aktor Serde           0.502 ms
Gloo                  3.251 ms
Yew                   3.385 ms
Leptos                3.237 ms
Leptos transferable   1.460 ms
```

### 10,485 records

An id and two strings each:

```text
Shared Wasm thread    0.035 ms
AktorData             1.969 ms
Aktor Serde           3.481 ms
Gloo                  4.451 ms
Yew                   3.553 ms
Leptos               20.748 ms
```

## AktorData

CPU time per encode / decode:

```text
                     Bytes       Encode       Decode
4,096 booleans          515      1.06 us       0.43 us
4,096 f64s           32,771      0.46 us       0.46 us
```

## TCP

2 MiB echo:

```text
Round trip    3.76 ms
CPU time      3.62 ms
```
