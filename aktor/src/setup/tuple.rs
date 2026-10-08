use super::*;
use core::{
    fmt,
    task::{Context, Poll},
};

#[doc(hidden)]
pub struct AktorPairStartup<A: AktorStart, B: AktorStart<Group = A::Group>> {
    context: AktorStartContext<A::Group>,
    first: Option<Pin<Box<A::Startup>>>,
    remaining: Option<Box<B>>,
    second: Option<Pin<Box<B::Startup>>>,
    handle: Option<Box<A::Handles>>,
}
impl<A: AktorStart, B: AktorStart<Group = A::Group>> Future for AktorPairStartup<A, B> {
    type Output = Result<(A::Handles, B::Handles), AktorPairStartError<A::Error, B::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(first) = &mut this.first {
            let handle = match first.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(AktorPairStartError::First(error)));
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
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(AktorPairStartError::Second(error)));
                }
                Poll::Ready(Ok(handle)) => handle,
            };

            this.second = None;

            if let Some(first) = this.handle.take() {
                return Poll::Ready(Ok((*first, handle)));
            }
        }

        Poll::Ready(Err(AktorPairStartError::Setup(AktorSetupError::new(
            "actor startup was polled after completion",
        ))))
    }
}

impl<A: AktorStart, B: AktorStart<Group = A::Group>> AktorStart for (A, B) {
    type Error = AktorPairStartError<A::Error, B::Error>;
    type Group = A::Group;
    type Handles = (A::Handles, B::Handles);
    type Startup = AktorPairStartup<A, B>;

    fn source_is_setup(error: &Self::Error) -> bool {
        match error {
            AktorPairStartError::First(error) => A::source_is_setup(error),
            AktorPairStartError::Second(error) => B::source_is_setup(error),
            AktorPairStartError::Setup(_) => false,
        }
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        self.0.begin(group)
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
pub struct AktorTupleStartup<Starting: Future, Handles, Error = AktorStartError> {
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

macro_rules! tuple {
    ($first:ident: $first_value:ident: $first_index:tt, $($type:ident: $value:ident: $index:tt),+) => {
        impl<$first: AktorStart, $($type: AktorStart<Group = $first::Group>),+>
            AktorStart for ($first, $($type,)+)
        {
            type Error = <nested!($first, $($type),+) as AktorStart>::Error;
            type Group = $first::Group;
            type Handles = ($first::Handles, $($type::Handles,)+);
            type Startup = AktorTupleStartup<
                <nested!($first, $($type),+) as AktorStart>::Startup,
                Self::Handles,
                Self::Error,
            >;

            fn source_is_setup(error: &Self::Error) -> bool {
                <nested!($first, $($type),+) as AktorStart>::source_is_setup(error)
            }

            fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
                self.$first_index.begin(group)
            }

            fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
                let ($first_value, $($value,)+) = self;
                AktorTupleStartup {
                    starting: Box::pin(nested!($first_value, $($value),+).start_in(context)),
                    flatten: |result| result.map(|nested!($first_value, $($value),+)| ($first_value, $($value,)+)),
                }
            }
        }
    };
}

tuple!(A: a: 0, B: b: 1, C: c: 2);
tuple!(A: a: 0, B: b: 1, C: c: 2, D: d: 3);
tuple!(A: a: 0, B: b: 1, C: c: 2, D: d: 3, E: e: 4);
tuple!(A: a: 0, B: b: 1, C: c: 2, D: d: 3, E: e: 4, F: f: 5);
tuple!(A: a: 0, B: b: 1, C: c: 2, D: d: 3, E: e: 4, F: f: 5, G: g: 6);
tuple!(A: a: 0, B: b: 1, C: c: 2, D: d: 3, E: e: 4, F: f: 5, G: g: 6, H: h: 7);

pub enum AktorPairStartError<A: core::error::Error + 'static, B: core::error::Error + 'static> {
    Setup(AktorSetupError),
    First(A),
    Second(B),
}
impl<A: core::error::Error + 'static, B: core::error::Error + 'static> fmt::Display
    for AktorPairStartError<A, B>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(_) => f.write_str("actor setup failed"),
            Self::First(error) => fmt::Display::fmt(error, f),
            Self::Second(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl<A: core::error::Error + 'static, B: core::error::Error + 'static> fmt::Debug
    for AktorPairStartError<A, B>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl<A: core::error::Error + 'static, B: core::error::Error + 'static> core::error::Error
    for AktorPairStartError<A, B>
{
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Setup(error) => Some(error),
            Self::First(error) => error.source(),
            Self::Second(error) => error.source(),
        }
    }
}
impl<A: core::error::Error + 'static, B: core::error::Error + 'static> From<AktorSetupError>
    for AktorPairStartError<A, B>
{
    fn from(error: AktorSetupError) -> Self {
        Self::Setup(error)
    }
}
