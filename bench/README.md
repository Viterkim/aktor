# Calling a counter

```sh
REPEATS=7 cargo run --manifest-path bench/Cargo.toml --release --locked
RUNTIME=multi REPEATS=7 cargo run --manifest-path bench/Cargo.toml --release --locked
cargo run --manifest-path bench/Cargo.toml --release --locked --features allocations
```

This benchmarks the experimental checkout. Its dependencies stay here, your app doesn't pull in the competing libraries.

Every owner uses the same counter. count increments it, cpu4096 also does a hash loop, yield awaits Tokio's yield_now. Each client waits for its reply before sending again. The checksum checks all the increments, startup and shutdown stay outside the measurement.

Calls use a queue of 100, with one or sixteen clients. CAPACITY changes it when BACKEND selects a bounded implementation. Actify fixes its queue at 100. The output gives the median and range over seven runs of 20,000 completed calls. With sixteen clients that's throughput cost per call, it doesn't give each caller's waiting time. CALLS and REPEATS change the run size, BACKEND picks one implementation. WORK picks count, cpu4096 or yield, CLIENTS picks the caller count. These filters also apply to allocation counting, which supports one caller.

For queue pressure, `BACKEND=aktor-task CAPACITY=1 CLIENTS=16 WORK=yield` runs the same calls with a small queue. Before timing it gates an operation, fills every waiting slot and polls one more submission until it blocks, then checks that all accepted calls drain. The probe prints on stderr. CSV includes capacity, with 0 for direct and mutex calls. Direct calls require CLIENTS=1 when selected explicitly. Unsupported selections fail before printing CSV.

hand-task is the [mpsc / oneshot loop](src/hand.rs), aktor-task is TokioTask. [Actify 0.9.0](https://docs.rs/actify/0.9.0/actify/) runs without state broadcasts and [Kameo 0.22.2](https://docs.rs/kameo/0.22.2/kameo/) uses a bounded mailbox. kameo-send uses its explicit `.send().await`, which avoids the boxed caller future in `.await`. They run on the same Tokio runtime, current thread by default, four runtime threads with RUNTIME=multi.

hand-thread runs that same manual loop on a dedicated thread. Compare it with aktor-thread (TokioThread) and aktor-std (StdThread). direct and direct-aktor call the function on owned state, mutex shares it behind a Tokio mutex. Caller cancellation has different effects there.

The manual loop has no failure reporting or shutdown watchdog. The actor libraries have their own lifecycle policies, the timings measure ordinary successful calls. Latest sessions and browser transport aren't exercised here.

Allocation counting runs only on the current thread. It counts all allocations during completed calls, including the small cost of launching the client task, and doesn't claim to measure allocations on another thread.

## Results here

Intel Core Ultra 9 285H, Linux, Rust 1.99.0, Tokio 1.53.2. Release build, seven runs pinned to CPU 2, one client on the current thread. Count and yield use 500,000 completed calls, cpu4096 uses 50,000:

- Handwritten task: 0.215 µs for count, 5.48 µs for cpu4096.
- TokioTask: 0.385 µs for count, 5.29 µs for cpu4096.
- Actify: 0.246 µs for count, 5.73 µs for cpu4096.
- Kameo: 0.450 µs for count, 5.93 µs for cpu4096.
- Kameo with explicit send: 0.441 µs for count, 5.94 µs for cpu4096.

The yielding call took 0.517 µs in Aktor against 0.301 µs by hand. With sixteen clients, empty calls cost 0.302 µs each in Aktor and 0.165 µs by hand, cpu4096 cost 5.69 µs and 5.67 µs. These are throughput figures, each client's reply waits behind the others. In the CPU workload, about 93% of perf samples landed in the operation itself. CPU frequency and background load varied, treat these as measurements on this laptop.

Perf samples put ordinary task costs in waking tasks, channel admission and the owned state lease. Callgrind counted about 4,650 instructions per empty Aktor task call before this pass, 2,150 by hand, 2,700 in Actify and 4,000 in Kameo. Replacing the task's one-time shutdown watches with oneshots and borrowing its sender during immediate admission reduced Aktor to about 4,200 instructions. Paired timing runs improved tiny calls by about 5 to 10%, yielding calls by about 3 to 10%. The earlier persistent waits and atomic shutdown flag had already reduced count from about 0.54 µs to 0.38 µs in a controlled run.

Keeping the operation future in its queued box removed another allocation and improved tiny task calls by roughly 3 to 6% in paired runs, including the cooperative submission check. Large futures also enlarge that queued box. With 64 idle actors, five seconds used about 0.19 ms of CPU for tasks, 0.80 ms for TokioThread and 1.85 ms for StdThread, including reading the counters. The polling loop experiments didn't give a reliable throughput gain and stayed out.

Strace found only a few hundred syscalls for 100,000 task calls, mostly startup. Dedicated owners made roughly two futex calls per round trip. Their runtime entry and watch registration also cost CPU time. Immediate admission, shared packet storage and a reusable thread waker reduced the final TokioThread count from about 12,700 to 10,200 instructions per call, StdThread from 9,450 to 7,710. Both builds used the same profiling flags, with caller and owner pinned to CPU 5.

Allocation counting finds two allocations per task call in Aktor, one by hand, four in Actify and five in Kameo, or four with explicit send. Aktor allocates 264 bytes for this input, Kameo's explicit send reduces its 1,032 bytes to 336. Actify allocates 232 bytes. These counts don't tell you how much queue and wake work each implementation does. Native async calls now share one packet allocation between the job and reply. Counting across all threads with Valgrind found two allocations and 272 bytes per call, down from four and 592 bytes in TokioThread, six and 640 bytes in StdThread after also reusing its waker.

For CPU samples, build with symbols and frame pointers, then run one workload:

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 RUSTFLAGS="-C force-frame-pointers=yes" \
    cargo build --manifest-path bench/Cargo.toml --release --locked --bin aktor-bench
WORK=count CLIENTS=1 BACKEND=aktor-task CALLS=10000000 REPEATS=1 \
    perf record --call-graph fp -- bench/target/release/aktor-bench
perf report
```

Build both versions with the same benchmark source, then alternate them with:

```sh
BACKEND=aktor-task WORK=count CLIENTS=1 CALLS=500000 REPEATS=7 \
    python3 bench/pair.py /path/before /path/after \
    --before-source BASE --after-source PATCH \
    --output bench/results/count.csv --cpu 2
```

It keeps every row and the build identities beside the CSV. Pass `--flags` for profiling flags, and include the patch identity for uncommitted changes. [Recorded pairs](results/) include the noisy runs too.

Instruction and allocation counts used the difference between runs of different lengths to remove startup costs. Timings came from runs without a profiler. Mutex fast paths and alternative service wake caching didn't give a reliable improvement, so they stayed out.

`cargo run --manifest-path bench/Cargo.toml --release --bin lifecycle -- 64 intervals` counts idle StdThread threads through Linux procfs, then shuts down with 33 admitted calls per actor. Run with 1, 8 or 64, leave out intervals for the plain case. It checks resource confinement, draining and helper termination over two cycles, then exercises failed startup.

Combining join observation with supervision reduced idle threads from 3 per actor to 2. For 64 actors that was 193 to 129 threads, or 257 to 193 with one interval each. Those counts include the group driver. The shared timer adds one thread once active, the watchdog starts during shutdown.
