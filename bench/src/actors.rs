use crate::work::{Counter, Work, step};
use actify::Handle;
use aktor::*;
use kameo::{
    Actor,
    actor::Spawn,
    message::{Context, Message},
};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

#[actify::actify(skip_broadcast)]
impl Counter {
    pub async fn actify_step(&mut self, work: Work) -> u64 {
        self.step(work).await
    }
}

#[derive(Actor)]
pub struct KameoCounter(pub Counter);
pub struct Step(pub Work);
impl Message<Step> for KameoCounter {
    type Reply = u64;

    async fn handle(&mut self, call: Step, _: &mut Context<Self, Self::Reply>) -> u64 {
        self.0.step(call.0).await
    }
}

pub enum Client {
    Direct(Arc<Mutex<Counter>>),
    Hand(mpsc::Sender<crate::hand::Call>),
    Task(AktorTask<Counter>),
    Thread(listener::Handle<Counter>),
    Actify(Handle<Counter>),
    Kameo(kameo::actor::ActorRef<KameoCounter>),
    KameoSend(kameo::actor::ActorRef<KameoCounter>),
}
impl Clone for Client {
    fn clone(&self) -> Self {
        match self {
            Self::Direct(value) => Self::Direct(value.clone()),
            Self::Hand(value) => Self::Hand(value.clone()),
            Self::Task(value) => Self::Task(value.new_handle()),
            Self::Thread(value) => Self::Thread(value.new_handle()),
            Self::Actify(value) => Self::Actify(value.clone()),
            Self::Kameo(value) => Self::Kameo(value.clone()),
            Self::KameoSend(value) => Self::KameoSend(value.clone()),
        }
    }
}
impl Client {
    pub async fn step(&self, work: Work) -> u64 {
        match self {
            Self::Direct(counter) => counter.lock().await.step(work).await,
            Self::Hand(sender) => crate::hand::step(sender, work).await,
            Self::Task(counter) => step(counter, work).await,
            Self::Thread(counter) => step(counter, work).await,
            Self::Actify(counter) => counter.actify_step(work).await,
            Self::Kameo(counter) => counter.ask(Step(work)).await.unwrap(),
            Self::KameoSend(counter) => counter.ask(Step(work)).send().await.unwrap(),
        }
    }
}

pub struct Owner {
    pub client: Client,
    pub group: Option<AktorGroup>,
    pub task: Option<tokio::task::JoinHandle<()>>,
    pub thread: Option<std::thread::JoinHandle<()>>,
    pub closed: Option<Arc<tokio::sync::Notify>>,
}
impl Owner {
    pub async fn new(name: &str, capacity: usize) -> Self {
        let options = Some(AktorOptions {
            capacity,
            ..Default::default()
        });
        let mut owner = Self {
            client: Client::Direct(Arc::new(Mutex::new(Counter::default()))),
            group: None,
            task: None,
            thread: None,
            closed: None,
        };

        match name {
            "mutex" => {}
            "hand-task" | "hand-thread" => {
                let (sender, receiver) = mpsc::channel(capacity);

                owner.client = Client::Hand(sender);

                if name == "hand-task" {
                    owner.task = Some(tokio::spawn(crate::hand::serve(receiver)));
                } else {
                    owner.thread = Some(std::thread::spawn(move || {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                            .block_on(crate::hand::serve(receiver));
                    }));
                }
            }
            "aktor-task" => {
                let actors = start(AktorSetup {
                    name: AktorName::new(name),
                    role: AktorNoRole,
                    kind: AktorKind::TokioTask,
                    closures: AktorClosures::new(async || Ok(Counter::default())),
                    options,
                })
                .await
                .unwrap();

                owner.client = Client::Task(actors.handles);
                owner.group = Some(actors.group);
            }
            "aktor-thread" => {
                let actors = start(AktorSetup {
                    name: AktorName::new(name),
                    role: AktorNoRole,
                    kind: AktorKind::TokioThread,
                    closures: AktorClosures::new(async || Ok(Counter::default())),
                    options,
                })
                .await
                .unwrap();

                owner.client = Client::Thread(actors.handles.new_handle());
                owner.group = Some(actors.group);
            }
            "aktor-std" => {
                let actors = start(AktorSetup {
                    name: AktorName::new(name),
                    role: AktorNoRole,
                    kind: AktorKind::StdThread,
                    closures: AktorClosures::new(async || Ok(Counter::default())),
                    options,
                })
                .await
                .unwrap();

                owner.client = Client::Thread(actors.handles.new_handle());
                owner.group = Some(actors.group);
            }
            "actify" => {
                let closed = Arc::new(tokio::sync::Notify::new());

                owner.client = Client::Actify(Handle::new(Counter {
                    closed: Some(closed.clone()),
                    ..Counter::default()
                }));
                owner.closed = Some(closed);
            }
            "kameo" | "kameo-send" => {
                let counter = KameoCounter::spawn_with_mailbox(
                    KameoCounter(Counter::default()),
                    kameo::mailbox::bounded(capacity),
                );

                counter.wait_for_startup().await;
                owner.client = if name == "kameo-send" {
                    Client::KameoSend(counter)
                } else {
                    Client::Kameo(counter)
                };
            }
            _ => panic!("unknown backend {name}"),
        }

        owner
    }

    pub async fn close(self) {
        let Self {
            client,
            group,
            task,
            thread,
            closed,
        } = self;

        if let Some(group) = group {
            let report = group.shutdown().await;
            assert!(!report.failed(), "{report}");
        }

        if let Client::Kameo(counter) | Client::KameoSend(counter) = &client {
            counter.stop_gracefully().await.unwrap();
            counter.wait_for_shutdown().await;
        }

        drop(client);

        if let Some(closed) = closed {
            closed.notified().await;
        }

        if let Some(task) = task {
            task.await.unwrap();
        }

        if let Some(thread) = thread {
            thread.join().unwrap();
        }
    }
}
