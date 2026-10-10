use super::*;
use core::task::{Context, Poll};

#[doc(hidden)]
pub struct AktorPairStartup<A: AktorStart<MA>, B: AktorStart<MB, Group = A::Group>, MA, MB> {
    mode: core::marker::PhantomData<fn() -> (MA, MB)>,
    context: AktorStartContext<A::Group>,
    first: Option<Pin<Box<A::Startup>>>,
    remaining: Option<Box<B>>,
    second: Option<Pin<Box<B::Startup>>>,
    handle: Option<Box<A::Handles>>,
}
impl<A: AktorStart<MA>, B: AktorStart<MB, Group = A::Group>, MA, MB> Future
    for AktorPairStartup<A, B, MA, MB>
{
    type Output = Result<(A::Handles, B::Handles), AktorStartError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(first) = &mut this.first {
            let handle = match first.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(error));
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
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(handle)) => handle,
            };

            this.second = None;

            if let Some(first) = this.handle.take() {
                return Poll::Ready(Ok((*first, handle)));
            }
        }

        Poll::Ready(Err(AktorStartError::Setup(AktorSetupError::new(
            "actor startup was polled after completion",
        ))))
    }
}

impl<A: AktorStart<MA>, B: AktorStart<MB, Group = A::Group>, MA, MB> AktorStart<(MA, MB)>
    for (A, B)
{
    type Group = A::Group;
    type Handles = (A::Handles, B::Handles);
    type Startup = AktorPairStartup<A, B, MA, MB>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        self.0.begin(group)
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        AktorPairStartup {
            mode: core::marker::PhantomData,
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
    ($first:ident: $first_mode:ident: $first_value:ident: $first_index:tt, $($type:ident: $mode:ident: $value:ident: $index:tt),+) => {
        impl<$first: AktorStart<$first_mode>, $first_mode, $($type: AktorStart<$mode, Group = $first::Group>, $mode),+>
            AktorStart<($first_mode, $($mode,)+)> for ($first, $($type,)+)
        {
            type Group = $first::Group;
            type Handles = ($first::Handles, $($type::Handles,)+);
            type Startup = AktorTupleStartup<
                <nested!($first, $($type),+) as AktorStart<nested!($first_mode, $($mode),+)>>::Startup,
                Self::Handles,
                AktorStartError,
            >;

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

tuple!(A: M0: a: 0, B: M1: b: 1, C: M2: c: 2);
tuple!(A: M0: a: 0, B: M1: b: 1, C: M2: c: 2, D: M3: d: 3);
tuple!(A: M0: a: 0, B: M1: b: 1, C: M2: c: 2, D: M3: d: 3, E: M4: e: 4);
tuple!(A: M0: a: 0, B: M1: b: 1, C: M2: c: 2, D: M3: d: 3, E: M4: e: 4, F: M5: f: 5);
tuple!(A: M0: a: 0, B: M1: b: 1, C: M2: c: 2, D: M3: d: 3, E: M4: e: 4, F: M5: f: 5, G: M6: g: 6);
tuple!(A: M0: a: 0, B: M1: b: 1, C: M2: c: 2, D: M3: d: 3, E: M4: e: 4, F: M5: f: 5, G: M6: g: 6, H: M7: h: 7);
