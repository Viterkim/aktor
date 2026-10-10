# Performance

An empty counter makes the messaging do almost all the work, so that's where Aktor's overhead sticks out. These numbers include sending the call and getting its result back. Startup and shutdown stay outside the timer.

## Native tasks

One caller, an empty counter and a Tokio runtime on one thread:

```text
Handwritten task       181 ns     1 allocation
Actify                 211 ns     4 allocations
Aktor TokioTask        320 ns     2 allocations
Kameo                  383 ns     5 allocations
Kameo explicit send    375 ns     4 allocations
```

## Dedicated threads

An empty call with the caller and owner sharing one CPU:

```text
Handwritten thread    1.517 microseconds
Aktor StdThread       2.001 microseconds
Aktor TokioThread     2.361 microseconds
```

TokioTask is the cheaper choice for lots of small actors. Dedicated threads let resources own their execution and finish cleanup after the caller's runtime exits.

## Browser workers

One caller sends owned Rust values to a real worker and gets them back. A 1 MiB byte round trip:

```text
AktorData             0.452 ms
Aktor Serde           0.507 ms
Gloo                  2.835 ms
Yew                   2.932 ms
Leptos                2.761 ms
Leptos transferable   1.241 ms
```

With 10,485 records, each containing an id and two strings:

```text
AktorData             2.131 ms
Aktor Serde           3.591 ms
Gloo                  4.062 ms
Yew                   3.471 ms
Leptos               24.061 ms
```

Large payloads are where AktorData does well. Calls with eight bytes took 156 microseconds through AktorData, 23 through Gloo, 18 through Yew and 26 through Leptos. Yew was quickest there.
