# Performance

## Calling a counter

Add one and wait for the answer. One caller, queue of 100, Tokio on one thread.

```text
Handwritten Tokio loop    0.19 µs
Actify                    0.23 µs
Aktor TokioTask           0.34 µs
Kameo .send()             0.40 µs
Kameo                     0.41 µs
```

Actify has broadcasts turned off.

## With CPU work

Same call, with a 4,096-step hash loop inside it.

```text
Handwritten Tokio loop    5.62 µs
Kameo .send()             5.72 µs
Kameo                     5.74 µs
Actify                    5.90 µs
Aktor TokioTask           5.98 µs
```

## With a yield

The counter call also awaits Tokio's yield_now().

```text
Handwritten Tokio loop    0.31 µs
Actify                    0.34 µs
Aktor TokioTask           0.46 µs
Kameo .send()             0.55 µs
Kameo                     0.57 µs
```

## Web workers

Send bytes to a worker in Chromium and get them back as Rust bytes. Aktor reuses its buffers automatically.

### 8 bytes

```text
Yew Agent               22 µs
Gloo Worker             22 µs
Leptos Workers          31 µs
Aktor, Serde            33 µs
Aktor, AktorData        35 µs
Leptos, transferable    37 µs
```

### 64 KiB

```text
Aktor, Serde           0.057 ms
Aktor, AktorData       0.057 ms
Leptos, transferable   0.095 ms
Leptos Workers          0.23 ms
Yew Agent               0.23 ms
Gloo Worker             0.26 ms
```

### 1 MiB

```text
Aktor, AktorData        0.37 ms
Aktor, Serde            0.41 ms
Leptos, transferable    1.35 ms
Leptos Workers          3.15 ms
Gloo Worker             3.24 ms
Yew Agent               3.38 ms
```

The transferable case uses Leptos' buffer adapter, including conversion to and from Rust bytes.

## Sending records

10,485 records with an id and two strings, sent to a worker and back.

```text
Aktor, AktorData         2.01 ms
Aktor, Serde             3.43 ms
Yew Agent                3.68 ms
Gloo Worker              4.54 ms
Leptos Workers          22.69 ms
```
