mod actors;
mod hand;
mod work;

use actors::{Client, Owner};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Barrier;
#[cfg(not(feature = "allocations"))]
use work::Counter;
use work::Work;

async fn sample(client: &Client, clients: usize, calls: usize, work: Work, prior: u64) -> Duration {
    let gate = Arc::new(Barrier::new(clients + 1));
    let mut tasks = Vec::new();

    for _ in 0..clients {
        let client = client.clone();
        let gate = gate.clone();

        tasks.push(tokio::spawn(async move {
            gate.wait().await;

            let mut sum = 0;

            for _ in 0..calls {
                sum += client.step(work).await;
            }

            sum
        }));
    }

    let start = Instant::now();

    gate.wait().await;

    let mut sum = 0;

    for task in tasks {
        sum += task.await.unwrap();
    }

    let elapsed = start.elapsed();
    let n = (clients * calls) as u64;

    assert_eq!(sum, n * prior + n * (n + 1) / 2);
    elapsed
}

#[cfg(not(feature = "allocations"))]
async fn measure(
    name: &str,
    clients: usize,
    calls: usize,
    work: Work,
    repeats: usize,
    capacity: usize,
) {
    let owner = Owner::new(name, capacity).await;

    if clients > capacity
        && let Client::Task(task) = &owner.client
    {
        pressure(task, capacity).await;
    }

    sample(&owner.client, 1, 1000, work, 0).await;

    let n = (clients * calls) as u64;
    let mut times = Vec::new();

    for repeat in 0..repeats {
        times.push(
            sample(
                &owner.client,
                clients,
                calls,
                work,
                1000 + repeat as u64 * n,
            )
            .await,
        );
    }

    owner.close().await;
    times.sort();

    let capacity = if name == "mutex" { 0 } else { capacity };
    let nanos = |duration: Duration| duration.as_nanos() as f64 / n as f64;

    println!(
        "{name},{},{clients},{capacity},{n},{:.1},{:.1},{:.1}",
        work.name(),
        nanos(times[repeats / 2]),
        nanos(times[0]),
        nanos(times[repeats - 1])
    );
}

#[cfg(not(feature = "allocations"))]
#[aktor::aktor]
async fn hold(
    _: &mut Counter,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
) {
    entered.notify_one();
    release.notified().await;
}

#[cfg(not(feature = "allocations"))]
#[aktor::aktor]
async fn reset(counter: &mut Counter) {
    counter.count = 0;
}

#[cfg(not(feature = "allocations"))]
async fn pressure(task: &aktor::AktorTask<Counter>, capacity: usize) {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let running = hold(task, entered.clone(), release.clone())
        .try_send()
        .unwrap();

    entered.notified().await;

    let mut waiting = Vec::new();

    for _ in 0..capacity {
        waiting.push(work::step(task, Work::Count).try_send().unwrap());
    }

    let admission = work::step(task, Work::Count).send();

    tokio::pin!(admission);
    assert!(
        poll_fn(|cx| Poll::Ready(admission.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    release.notify_one();

    let last = admission.await;

    running.await;

    for (index, reply) in waiting.into_iter().enumerate() {
        assert_eq!(reply.await, index as u64 + 1);
    }

    assert_eq!(last.await, capacity as u64 + 1);
    reset(task).await;
    eprintln!("pressure probe: {capacity} waiting calls, one blocked admission, all drained");
}

#[cfg(not(feature = "allocations"))]
async fn direct(work: Work, calls: usize, repeats: usize, annotated: bool) {
    let mut times = Vec::new();

    for _ in 0..repeats {
        let mut counter = Counter::default();
        let start = Instant::now();
        let mut sum = 0;

        for _ in 0..calls {
            sum += if annotated {
                work::step(&mut counter, work).await
            } else {
                counter.step(work).await
            };
        }

        times.push(start.elapsed());

        let n = calls as u64;

        assert_eq!(sum, n * (n + 1) / 2);
    }

    times.sort();

    let name = if annotated { "direct-aktor" } else { "direct" };
    let nanos = |duration: Duration| duration.as_nanos() as f64 / calls as f64;

    println!(
        "{name},{},1,0,{calls},{:.1},{:.1},{:.1}",
        work.name(),
        nanos(times[repeats / 2]),
        nanos(times[0]),
        nanos(times[repeats - 1])
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repeats: usize = std::env::var("REPEATS")
        .unwrap_or_else(|_| "5".into())
        .parse()?;
    let calls: usize = std::env::var("CALLS")
        .unwrap_or_else(|_| "20000".into())
        .parse()?;

    if calls < 16 || repeats == 0 {
        return Err("CALLS must be at least 16 and REPEATS must be positive".into());
    }

    let backend = std::env::var("BACKEND").ok();
    let workload = std::env::var("WORK").ok();
    let clients = std::env::var("CLIENTS")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?;

    if clients.is_some_and(|count| count == 0 || count > calls) {
        return Err("CLIENTS must be positive and no greater than CALLS".into());
    }

    let works: Vec<_> = [Work::Count, Work::Cpu(4096), Work::Yield]
        .into_iter()
        .filter(|work| workload.as_ref().is_none_or(|name| *name == work.name()))
        .collect();

    if works.is_empty() {
        return Err("unknown workload".into());
    }

    let selected = |name: &str| backend.as_ref().is_none_or(|filter| filter == name);
    #[cfg(feature = "allocations")]
    let backends = [
        "mutex",
        "hand-task",
        "aktor-task",
        "actify",
        "kameo",
        "kameo-send",
    ]
    .as_slice();

    #[cfg(not(feature = "allocations"))]
    let backends = [
        "direct",
        "direct-aktor",
        "mutex",
        "hand-task",
        "aktor-task",
        "actify",
        "kameo",
        "kameo-send",
        "hand-thread",
        "aktor-thread",
        "aktor-std",
    ]
    .as_slice();

    if let Some(name) = &backend
        && !backends.contains(&name.as_str())
    {
        return Err(format!("unsupported backend for this mode: {name}").into());
    }

    if backend
        .as_deref()
        .is_some_and(|name| name.starts_with("direct"))
        && clients.is_some_and(|count| count != 1)
    {
        return Err("direct calls require CLIENTS=1".into());
    }

    let capacity = std::env::var("CAPACITY").ok();

    if capacity.is_some()
        && !backend.as_deref().is_some_and(|name| {
            matches!(
                name,
                "hand-task"
                    | "hand-thread"
                    | "aktor-task"
                    | "aktor-thread"
                    | "aktor-std"
                    | "kameo"
                    | "kameo-send"
            )
        })
    {
        return Err("CAPACITY needs a bounded backend selected with BACKEND (Actify fixes its capacity at 100)".into());
    }

    let capacity = capacity.map_or(Ok(100), |value| value.parse::<usize>())?;

    if capacity == 0 {
        return Err("CAPACITY must be positive".into());
    }

    let runtime = std::env::var("RUNTIME").unwrap_or_else(|_| "current".into());
    let mut builder = if runtime == "multi" {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(4);
        builder
    } else {
        if runtime != "current" {
            return Err("RUNTIME must be current or multi".into());
        }

        tokio::runtime::Builder::new_current_thread()
    };

    #[cfg(feature = "allocations")]
    if runtime != "current" || clients.is_some_and(|count| count != 1) {
        return Err("allocation counting requires RUNTIME=current and CLIENTS=1".into());
    }

    let runtime = builder.enable_all().build()?;

    #[cfg(feature = "allocations")]
    {
        println!("backend,work,capacity,calls,allocations_per_call,bytes_per_call");

        for work in works {
            for &name in backends {
                if !selected(name) {
                    continue;
                }

                let owner = runtime.block_on(Owner::new(name, capacity));

                runtime.block_on(sample(&owner.client, 1, 1000, work, 0));

                let info = allocation_counter::measure(|| {
                    runtime.block_on(sample(&owner.client, 1, calls, work, 1000));
                });

                runtime.block_on(owner.close());

                let capacity = if name == "mutex" { 0 } else { capacity };

                println!(
                    "{name},{},{capacity},{calls},{:.3},{:.1}",
                    work.name(),
                    info.count_total as f64 / calls as f64,
                    info.bytes_total as f64 / calls as f64
                );
            }
        }
    }
    #[cfg(not(feature = "allocations"))]
    runtime.block_on(async {
        println!(
            "backend,work,clients,capacity,calls,median_ns_per_call,min_ns_per_call,max_ns_per_call"
        );

        for work in works {
            if selected("direct") {
                direct(work, calls, repeats, false).await;
            }

            if selected("direct-aktor") {
                direct(work, calls, repeats, true).await;
            }

            for clients in clients.map_or_else(|| vec![1, 16], |clients| vec![clients]) {
                for &name in backends {
                    if name.starts_with("direct") || !selected(name) {
                        continue;
                    }

                    measure(name, clients, calls / clients, work, repeats, capacity).await;
                }
            }
        }
    });

    Ok(())
}
