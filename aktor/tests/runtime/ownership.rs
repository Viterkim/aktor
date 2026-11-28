use super::*;
use core::future::IntoFuture;
use core::num::NonZeroUsize;

#[cfg(feature = "testing")]
mod native {
    use super::*;
    use aktor::listener::WeakHandle;
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

    struct SelfLinked(Option<WeakHandle<SelfLinked>>);

    #[tokio::test]
    async fn weak_self() {
        let (actor, group) = thread_actor(SelfLinked(None), 1).await;
        let weak = actor.handle().downgrade();
        let link = weak.clone();
        call(&actor, async move |s| s.0 = Some(link)).await.unwrap();

        assert!(weak.upgrade().is_some());
        let completed = actor.completion();
        drop(actor);
        drop(group);
        tokio::time::timeout(Duration::from_secs(1), completed.wait())
            .await
            .unwrap()
            .unwrap();

        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn cancel() {
        let (actor, _group) = thread_actor(Vec::new(), 1).await;
        let release = hold(&actor).await;
        drop(call(&actor, async |rows| rows.push("unpolled")));
        let first = call(&actor, async |rows| rows.push("first"))
            .send()
            .await
            .unwrap();

        let drops = Arc::new(AtomicUsize::new(0));
        let input = Count(drops.clone());
        let mut canceled = call(&actor, async move |rows| {
            drop(input);
            rows.push("canceled");
        });
        assert!(poll(&mut canceled).is_pending());
        drop(canceled);
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        release.send(()).unwrap();
        first.await.unwrap();
        assert_eq!(
            call(&actor, async |rows| rows.clone()).await,
            Ok(vec!["first"])
        );
    }

    #[tokio::test]
    async fn closed() {
        let (actor, _group) = thread_actor((), 1).await;
        let release = hold(&actor).await;
        let drops = Arc::new(AtomicUsize::new(0));
        let input = Count(drops.clone());
        let reply = call(&actor, async move |_| drop(input))
            .send()
            .await
            .unwrap();

        let input = Count(drops.clone());
        let mut waiting = call(&actor, async move |_| drop(input));
        assert!(poll(&mut waiting).is_pending());

        actor.retire();
        release.send(()).unwrap();
        assert!(matches!(
            reply.await,
            Err(aktor::AktorDeliveryError {
                outcome: message::AktorDeliveryOutcome::Discarded,
                ..
            })
        ));
        assert!(matches!(
            waiting.send().await,
            Err(aktor::AktorDeliveryError {
                outcome: message::AktorDeliveryOutcome::NotAdmitted,
                ..
            })
        ));
        actor.completion().wait().await.unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn abandon() {
        let (actor, _group) = thread_actor(0, 1).await;
        let release = hold(&actor).await;
        let mut reply = call(&actor, async |state| *state += 1)
            .send()
            .await
            .unwrap();

        let wake = Arc::new(support::CountWake::default());
        let weak = Arc::downgrade(&wake);
        let waker = Waker::from(wake);
        assert!(
            Pin::new(&mut reply)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        drop(reply);
        assert!(weak.upgrade().is_none());

        release.send(()).unwrap();
        assert_eq!(call(&actor, async |s| *s).await, Ok(1));
    }

    #[tokio::test]
    async fn output() {
        let (actor, _group) = thread_actor((), 1).await;
        for finish_first in [false, true] {
            let release = hold(&actor).await;
            let drops = Arc::new(AtomicUsize::new(0));
            let input = Count(drops.clone());
            let reply = call(&actor, async move |_| input).send().await.unwrap();

            if finish_first {
                release.send(()).unwrap();
                call(&actor, async |_| ()).await.unwrap();
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                drop(reply);
            } else {
                drop(reply);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                release.send(()).unwrap();
                call(&actor, async |_| ()).await.unwrap();
            }
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }

        for take in [false, true] {
            let drops = Arc::new(AtomicUsize::new(0));
            let input = Count(drops.clone());
            let mut reply = call(&actor, async move |_| input).send().await.unwrap();
            call(&actor, async |_| ()).await.unwrap();

            let output = if take {
                let output = reply.try_take().unwrap();
                assert!(reply.try_take().is_none());
                output
            } else {
                reply.await
            };
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(output);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let input = Count(drops.clone());
        let reply = call(&actor, async move |_| {
            let _input = input;
            panic!("operation failed");
        })
        .send()
        .await
        .unwrap();
        assert!(matches!(
            reply.await,
            Err(aktor::AktorDeliveryError {
                outcome: message::AktorDeliveryOutcome::OutcomeUnknown,
                ..
            })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reply_wake() {
        let (actor, _group) = thread_actor(85, 1).await;
        let release = hold(&actor).await;
        let mut reply = call(&actor, async |s| *s).send().await.unwrap();
        let count = Arc::new(support::CountWake::default());
        let waker = Waker::from(count.clone());

        assert!(
            Pin::new(&mut reply)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(reply.try_take().is_none());

        release.send(()).unwrap();
        call(&actor, async |_| ()).await.unwrap();
        assert!(
            count.0.load(Ordering::Relaxed) > 0,
            "try_take replaced the reply waker"
        );
        assert_eq!(reply.try_take(), Some(Ok(85)));
    }
}

#[cfg(all(feature = "local", feature = "macros"))]
#[tokio::test]
async fn local_waiting() {
    use local_values::{Input, add};
    use std::{cell::Cell, rc::Rc};

    let (handle, owner) = local::channel::<Rc<Cell<u32>>, ()>(NonZeroUsize::MIN);
    let state = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let input = |value| Input {
        drops: drops.clone(),
        value,
    };
    let first = add(&handle, input(1)).send().await.unwrap();
    let mut abandoned = add(&handle, input(100)).into_future();
    assert!(poll(&mut abandoned).is_pending());
    drop(abandoned);
    assert_eq!(drops.get(), 1);

    let mut waiting = add(&handle, input(10)).into_future();
    assert!(poll(&mut waiting).is_pending());
    let mut running = Box::pin(owner.run(state.clone(), async |_| Ok(())));
    assert!(poll(&mut running).is_pending());
    assert_eq!(first.await.unwrap().0.get(), 1);
    let reply = waiting.send().await.unwrap();
    assert!(poll(&mut running).is_pending());
    assert_eq!(reply.await.unwrap().0.get(), 11);
    assert_eq!(drops.get(), 3);

    let mut admitted = add(&handle, input(1000)).into_future();
    assert!(poll(&mut admitted).is_pending());
    handle.shutdown();
    let reply = admitted.send().await.unwrap();
    running.await.unwrap();
    assert_eq!(reply.await.unwrap().0.get(), 1011);
    assert_eq!(drops.get(), 4);
}

#[cfg(all(feature = "local", feature = "macros"))]
#[tokio::test]
async fn local_consumed() {
    use local_values::count;
    use std::{cell::Cell, rc::Rc};

    for extract in 0..2 {
        let (handle, owner) = local::channel::<Rc<Cell<u32>>, ()>(NonZeroUsize::MIN);
        let state = Rc::new(Cell::new(0));
        let mut running = Box::pin(owner.run(state.clone(), async |_| Ok(())));
        let mut request = count(&handle).into_future();
        assert!(poll(&mut request).is_pending());
        assert!(poll(&mut running).is_pending());
        (&mut request).await.unwrap();

        let panic = match extract {
            0 => panics(async { drop(request.send().await.unwrap()) }).await,
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

    #[aktor(latest)]
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
        let (handle, owner) = cross_core::channel::<Rc<Cell<u32>>>(NonZeroUsize::MIN);
        let state = Rc::new(Cell::new(0));
        let mut running = Box::pin(owner.run_with(async || Ok(state.clone()), async |_| Ok(())));
        let mut request = count(&handle).into_future();
        assert!(poll(&mut request).is_pending());
        assert!(poll(&mut running).is_pending());
        (&mut request).await.unwrap();

        let panic = match extract {
            0 => panics(async { drop(request.send().await.unwrap()) }).await,
            _ => panics(request.cast()).await,
        };
        assert!(panic);
        assert_eq!(state.get(), 1);
        handle.shutdown();
        running.await.unwrap();
    }
}
