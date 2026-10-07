use super::*;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Waker},
};

struct Count(Arc<AtomicUsize>);
impl Drop for Count {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn request<S, F, I, O>(
    handle: &Handle<S>,
    function: F,
    input: I,
    asynchronous: bool,
) -> Request<'_, S, O>
where
    S: 'static,
    F: FnOnce(&mut S, I) -> O + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    if asynchronous {
        message::call_async(
            handle,
            async move |state, input| function(state, input),
            input,
        )
    } else {
        call(handle, function, input)
    }
}

struct SelfLinked {
    weak: WeakHandle<SelfLinked>,
}

#[tokio::test]
async fn weak_self() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (handle, listener) = channel::<SelfLinked>(1).unwrap();
            let weak = handle.downgrade();
            let actor = tokio::task::spawn_local(listener.run(SelfLinked { weak: weak.clone() }));

            assert!(weak.upgrade().is_ok());
            drop(handle);

            let state = tokio::time::timeout(std::time::Duration::from_secs(1), actor)
                .await
                .unwrap()
                .unwrap();

            assert!(state.weak.upgrade().is_err());
        })
        .await
}

#[tokio::test]
async fn cancel() {
    for asynchronous in [false, true] {
        tokio::task::LocalSet::new()
            .run_until(async {
                let (handle, listener) = channel::<Vec<&'static str>>(1).unwrap();

                drop(request(
                    &handle,
                    |rows, ()| rows.push("unpolled"),
                    (),
                    asynchronous,
                ));

                let mut first = Box::pin(request(
                    &handle,
                    |rows: &mut Vec<&'static str>, row| rows.push(row),
                    "first",
                    asynchronous,
                ));

                assert!(poll(first.as_mut()).is_pending());

                let input_drops = Arc::new(AtomicUsize::new(0));
                let mut canceled = Box::pin(request(
                    &handle,
                    |rows: &mut Vec<&'static str>, input: Count| {
                        drop(input);
                        rows.push("canceled");
                    },
                    Count(Arc::clone(&input_drops)),
                    asynchronous,
                ));

                assert!(poll(canceled.as_mut()).is_pending());
                drop(canceled);

                assert_eq!(input_drops.load(Ordering::SeqCst), 1);

                let actor = tokio::task::spawn_local(listener.run(Vec::new()));

                first.await;
                drop(handle);

                assert_eq!(actor.await.unwrap(), ["first"]);
            })
            .await
    }
}

#[tokio::test]
async fn closed() {
    for asynchronous in [false, true] {
        let (handle, listener) = channel::<()>(1).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let reply = request(
            &handle,
            |_, input| drop(input),
            Count(drops.clone()),
            asynchronous,
        )
        .send()
        .await;

        let mut waiting = request(
            &handle,
            |_, input| drop(input),
            Count(drops.clone()),
            asynchronous,
        );

        assert!(poll(&mut waiting).is_pending());

        drop(listener);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(panics(reply).await);
        assert!(panics(waiting).await);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
}

struct Wake;
// Needs an owned waker so its destruction is observable.
#[allow(clippy::manual_noop_waker)]
impl std::task::Wake for Wake {
    fn wake(self: Arc<Self>) {}
}

#[tokio::test]
async fn abandon() {
    for asynchronous in [false, true] {
        let (handle, listener) = channel::<usize>(1).unwrap();
        let mut reply = request(&handle, |state, ()| *state += 1, (), asynchronous)
            .send()
            .await;

        let wake = Arc::new(Wake);
        let weak = Arc::downgrade(&wake);
        let waker = Waker::from(wake);

        assert!(
            Pin::new(&mut reply)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        drop(reply);

        assert!(weak.upgrade().is_none(), "abandoned reply retained a waker");

        drop(handle);
        assert_eq!(listener.run(0).await, 1);
    }
}

#[tokio::test]
async fn output() {
    for asynchronous in [false, true] {
        tokio::task::LocalSet::new()
            .run_until(async {
                for finish_first in [false, true] {
                    let (handle, mut listener) = channel::<()>(1).unwrap();
                    let drops = Arc::new(AtomicUsize::new(0));
                    let reply = request(
                        &handle,
                        |_, input| input,
                        Count(drops.clone()),
                        asynchronous,
                    )
                    .send()
                    .await;

                    if finish_first {
                        listener.recv().await.unwrap().run(&mut ()).await;
                        assert_eq!(drops.load(Ordering::SeqCst), 0);
                        drop(reply);
                    } else {
                        drop(reply);
                        assert_eq!(drops.load(Ordering::SeqCst), 0);
                        listener.recv().await.unwrap().run(&mut ()).await;
                    }

                    assert_eq!(drops.load(Ordering::SeqCst), 1);
                }

                for take in [false, true] {
                    let (handle, mut listener) = channel::<()>(1).unwrap();
                    let drops = Arc::new(AtomicUsize::new(0));
                    let mut reply = request(
                        &handle,
                        |_, input| input,
                        Count(drops.clone()),
                        asynchronous,
                    )
                    .send()
                    .await;

                    listener.recv().await.unwrap().run(&mut ()).await;

                    let output = if take {
                        let output = reply.try_take().unwrap();

                        assert!(reply.try_take().is_none());
                        drop(reply);
                        output
                    } else {
                        reply.await
                    };

                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    drop(output);
                    assert_eq!(drops.load(Ordering::SeqCst), 1);
                }

                let (handle, mut listener) = channel::<()>(1).unwrap();

                listener.failure = FailurePolicy::Unwind;

                let drops = Arc::new(AtomicUsize::new(0));
                let reply = request(
                    &handle,
                    |_, _input| panic!("operation failed"),
                    Count(drops.clone()),
                    asynchronous,
                )
                .send()
                .await;

                let task = tokio::task::spawn_local(listener.run(()));

                assert!(task.await.unwrap_err().is_panic());

                drop(reply);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            })
            .await
    }
}

#[cfg(all(feature = "local", feature = "macros"))]
#[tokio::test]
async fn local_waiting() {
    use local_values::{Input, add};
    use std::{cell::Cell, rc::Rc};

    let (handle, owner) = local::channel::<Rc<Cell<u32>>, 1, ()>().unwrap();
    let state = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let input = |value| Input {
        drops: drops.clone(),
        value,
    };
    let first = add(&handle, input(1)).send().await;
    let mut abandoned = add(&handle, input(100));
    assert!(poll(&mut abandoned).is_pending());
    drop(abandoned);
    assert_eq!(drops.get(), 1);
    assert_eq!(state.get(), 0);

    let mut waiting = add(&handle, input(10)).into_request();
    assert!(poll(&mut waiting).is_pending());
    let mut running = Box::pin(owner.run(state.clone(), async |_| Ok(())));
    assert!(poll(&mut running).is_pending());
    assert_eq!(first.await.0.get(), 1);
    let reply = waiting.send().await;
    assert!(poll(&mut running).is_pending());
    assert_eq!(reply.await.0.get(), 11);
    assert_eq!(drops.get(), 3);

    let mut admitted = add(&handle, input(1000)).into_request();
    assert!(poll(&mut admitted).is_pending());
    handle.shutdown();
    let reply = admitted.send().await;
    running.await.unwrap();
    assert_eq!(reply.await.0.get(), 1011);
    assert_eq!(drops.get(), 4);
}

#[cfg(all(feature = "local", feature = "macros"))]
#[tokio::test]
async fn local_consumed() {
    use local_values::count;
    use std::{cell::Cell, rc::Rc};

    for extract in 0..2 {
        let (handle, owner) = local::channel::<Rc<Cell<u32>>, 1, ()>().unwrap();
        let state = Rc::new(Cell::new(0));
        let mut running = Box::pin(owner.run(state.clone(), async |_| Ok(())));
        let mut request = count(&handle).into_request();
        assert!(poll(&mut request).is_pending());
        assert!(poll(&mut running).is_pending());
        (&mut request).await;

        let panic = match extract {
            0 => panics(async { drop(request.send().await) }).await,
            _ => panics(request.cast()).await,
        };
        assert!(panic);
        assert_eq!(state.get(), 1);
        handle.shutdown();
        running.await.unwrap();
    }
}

#[cfg(all(feature = "local", feature = "macros"))]
mod local_values {
    use super::*;
    use std::{cell::Cell, rc::Rc};

    pub struct Input {
        pub drops: Rc<Cell<usize>>,
        pub value: u32,
    }
    impl Drop for Input {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    pub struct Output(pub Rc<Cell<u32>>);

    #[aktor]
    pub async fn add(state: &mut Rc<Cell<u32>>, input: Input) -> Output {
        state.set(state.get() + input.value);
        Output(state.clone())
    }

    #[aktor]
    pub async fn count(state: &mut Rc<Cell<u32>>) {
        state.set(state.get() + 1);
    }
}

#[cfg(all(feature = "embassy_cross_core", feature = "macros"))]
#[tokio::test]
async fn cross_core_consumed() {
    use local_values::count;
    use std::{cell::Cell, rc::Rc};

    for extract in 0..2 {
        let (handle, owner) = cross_core::channel::<Rc<Cell<u32>>>(1).unwrap();
        let state = Rc::new(Cell::new(0));
        let mut running = Box::pin(owner.run_with(async || Ok(state.clone()), async |_| Ok(())));
        let mut request = count(&handle).into_request();
        assert!(poll(&mut request).is_pending());
        assert!(poll(&mut running).is_pending());
        (&mut request).await;

        let panic = match extract {
            0 => panics(async { drop(request.send().await) }).await,
            _ => panics(request.cast()).await,
        };
        assert!(panic);
        assert_eq!(state.get(), 1);
        handle.shutdown();
        running.await.unwrap();
    }
}
