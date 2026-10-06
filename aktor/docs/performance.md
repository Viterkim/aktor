# Performance

Tiny calls make the overhead easy to see. The handwritten loop has no group failure reporting or shutdown watchdog, so the comparisons don't all buy you the same behaviour.

## Task calls

One caller, capacity 100, current-thread Tokio. Time per completed call.

```text
                   Counter    CPU work    Yielding
Handwritten        0.19 µs     5.62 µs     0.31 µs
Actify             0.23 µs     5.90 µs     0.34 µs
Aktor TokioTask    0.34 µs     5.98 µs     0.46 µs
Kameo .send()      0.40 µs     5.72 µs     0.55 µs
Kameo              0.41 µs     5.74 µs     0.57 µs
```

Actify uses skip_broadcast. These calls leave queue capacity available.

## Workers

Full round trips through BrowserWebWorker, before and after buffer reuse.

```text
AktorData               Fresh buffers    Reused buffers
64 KiB bytes                0.129 ms          0.054 ms
1 MiB bytes                  1.39 ms           0.34 ms
10,485 records               2.63 ms           2.02 ms
```

The 1 MiB run used about 78% less browser CPU. Tiny calls stayed around 27 µs.

Both codecs are binary. With reused buffers, the same records took:

```text
AktorData           2.02 ms     880 KB encoded
Tagged Serde        3.41 ms    1.53 MB encoded
```

## Batches

Small items sent together in one worker operation, time per item.

```text
  1 item           71.5 µs
 16 items           3.55 µs
128 items           1.31 µs
```

Another busy Chromium run shared the machine during this batch test. A batch has one operation's hooks and failure boundary.

Linux, October 2026, Rust 1.99.0, Chromium 153. Medians of seven native repeats on CPU 2 and six alternating worker pairs. Batches used four repeats. Runners and raw results live in the adjacent aktor-extras checkout.
