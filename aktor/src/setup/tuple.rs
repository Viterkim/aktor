use super::*;
use core::task::{Context, Poll};

#[doc(hidden)]
pub struct AktorPairStartup<A: AktorStart, B: AktorStart<Group = A::Group>> {
    context: AktorStartContext<A::Group>,
    first: Option<Pin<Box<A::Startup>>>,
    remaining: Option<Box<B>>,
    second: Option<Pin<Box<B::Startup>>>,
    handle: Option<Box<A::Handles>>,
}
impl<A: AktorStart, B: AktorStart<Group = A::Group>> Future for AktorPairStartup<A, B> {
    type Output =
        Result<(A::Handles, B::Handles), AktorStartError<AktorBranch<A::Error, B::Error>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(first) = &mut this.first {
            let handle = match first.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(error.map_init(AktorBranch::First)));
                }
                Poll::Ready(Ok(handle)) => handle,
            };

            this.first = None;
            this.handle = Some(Box::new(handle));

            if let Some(second) = this.remaining.take() {
                this.second = Some(Box::pin(second.start_in(this.context.clone())));
            }
        }

        if let Some(second) = &mut this.second {
            let handle = match second.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(mut error)) => {
                    if matches!(error.init, AktorInitOutcome::NotRun) {
                        error.init = AktorInitOutcome::Unconfirmed;
                    }

                    return Poll::Ready(Err(error.map_init(AktorBranch::Second)));
                }
                Poll::Ready(Ok(handle)) => handle,
            };

            this.second = None;

            if let Some(first) = this.handle.take() {
                return Poll::Ready(Ok((*first, handle)));
            }
        }

        Poll::Ready(Err(AktorStartCause::Setup(AktorError::new(
            "actor startup was polled after completion",
            (),
        ))
        .into()))
    }
}

impl<A: AktorStart, B: AktorStart<Group = A::Group>> AktorStart for (A, B) {
    type Group = A::Group;
    type Error = AktorBranch<A::Error, B::Error>;
    type Handles = (A::Handles, B::Handles);
    type Startup = AktorPairStartup<A, B>;

    fn needs_tokio_group(&self) -> bool {
        self.0.needs_tokio_group() || self.1.needs_tokio_group()
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        if self.1.needs_tokio_group() {
            self.1.begin(group)
        } else {
            self.0.begin(group)
        }
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        AktorPairStartup {
            first: Some(Box::pin(self.0.start_in(context.clone()))),
            context,
            remaining: Some(Box::new(self.1)),
            second: None,
            handle: None,
        }
    }
}

#[doc(hidden)]
pub struct AktorTupleStartup<Starting: Future, Handles, Error = AktorStartCause> {
    starting: Pin<Box<Starting>>,
    flatten: fn(Starting::Output) -> Result<Handles, Error>,
}
impl<Starting: Future, Handles, Error> AktorTupleStartup<Starting, Handles, Error> {
    pub fn new(
        starting: Starting,
        flatten: fn(Starting::Output) -> Result<Handles, Error>,
    ) -> Self {
        Self {
            starting: Box::pin(starting),
            flatten,
        }
    }
}
impl<Starting: Future, Handles, Error> Future for AktorTupleStartup<Starting, Handles, Error> {
    type Output = Result<Handles, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.starting.as_mut().poll(cx).map(this.flatten)
    }
}

macro_rules! nested {
    ($last:ident) => { $last };
    ($first:ident, $($rest:ident),+) => { ($first, nested!($($rest),+)) };
}

// Pairs drive startup recursively. Give the caller the same flat positions as the handles.
macro_rules! branch {
    ($error:expr, $first:ident, $last:ident) => {
        match $error {
            AktorBranch::First(error) => AktorBranch::$first(error),
            AktorBranch::Second(error) => AktorBranch::$last(error),
        }
    };
    ($error:expr, $first:ident, $($rest:ident),+) => {
        match $error {
            AktorBranch::First(error) => AktorBranch::$first(error),
            AktorBranch::Second(error) => branch!(error, $($rest),+),
        }
    };
}

macro_rules! tuple {
    ($first:ident: $first_value:ident: $first_case:ident, $($type:ident: $value:ident: $case:ident),+) => {
        impl<$first: AktorStart, $($type: AktorStart<Group = $first::Group>),+>
            AktorStart for ($first, $($type,)+)
        {
            type Group = $first::Group;
            type Error = AktorBranch<$first::Error, $($type::Error),+>;
            type Handles = ($first::Handles, $($type::Handles,)+);
            type Startup = AktorTupleStartup<
                <nested!($first, $($type),+) as AktorStart>::Startup,
                Self::Handles,
                AktorStartError<Self::Error>,
            >;

            fn needs_tokio_group(&self) -> bool {
                let ($first_value, $($value,)+) = self;
                $first_value.needs_tokio_group() $(|| $value.needs_tokio_group())+
            }

            fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
                let ($first_value, $($value,)+) = self;

                $(if $value.needs_tokio_group() {
                    return $value.begin(group);
                })+

                $first_value.begin(group)
            }

            fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
                let ($first_value, $($value,)+) = self;
                AktorTupleStartup {
                    starting: Box::pin(nested!($first_value, $($value),+).start_in(context)),
                    flatten: |result| result.map(|nested!($first_value, $($value),+)| ($first_value, $($value,)+))
                        .map_err(|error| error.map_init(|error| branch!(error, $first_case, $($case),+))),
                }
            }
        }
    };
}

tuple!(A: a: First, B: b: Second, C: c: Third);
tuple!(A: a: First, B: b: Second, C: c: Third, D: d: Fourth);
tuple!(A: a: First, B: b: Second, C: c: Third, D: d: Fourth, E: e: Fifth);
tuple!(A: a: First, B: b: Second, C: c: Third, D: d: Fourth, E: e: Fifth, F: f: Sixth);
tuple!(A: a: First, B: b: Second, C: c: Third, D: d: Fourth, E: e: Fifth, F: f: Sixth, G: g: Seventh);
tuple!(A: a: First, B: b: Second, C: c: Third, D: d: Fourth, E: e: Fifth, F: f: Sixth, G: g: Seventh, H: h: Eighth);
