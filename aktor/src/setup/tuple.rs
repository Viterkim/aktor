use super::*;

// A tuple launches every actor right away, in order, so their `start`s run side by side. A call
// from a later `start` into an earlier actor just queues until that one is ready. Shutdown still
// goes in reverse order.
impl<A: AktorStart, B: AktorStart<Group = A::Group>> AktorStart for (A, B) {
    type Group = A::Group;
    type Error = AktorBranch<A::Error, B::Error>;
    type Handles = (A::Handles, B::Handles);

    fn needs_tokio_group(&self) -> bool {
        self.0.needs_tokio_group() || self.1.needs_tokio_group()
    }

    fn actor_name(&self) -> &str {
        self.0.actor_name()
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        if self.1.needs_tokio_group() {
            begin_member(&self.1, group)
        } else {
            begin_member(&self.0, group)
        }
    }

    fn closed(name: &str, group: &Self::Group) -> Self::Handles {
        (A::closed(name, group), B::closed(name, group))
    }

    fn fallback(&self) -> impl FnOnce(&Self::Group) -> Self::Handles + use<A, B> {
        let first = self.0.fallback();
        let second = self.1.fallback();

        move |group| {
            (
                first(&group.fallback_member(Some(0))),
                second(&group.fallback_member(Some(1))),
            )
        }
    }

    fn launch(self, context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
        let mut first_context = context.clone();
        first_context.group.track_member();
        let first_kill = first_context.group.killswitch();
        let first_name = String::from(self.0.actor_name());

        let (first, first_ready) = match self.0.launch(first_context) {
            Ok((handle, ready)) => (
                handle,
                A::Group::observe_ready(ready, first_kill.clone(), first_name),
            ),
            Err(mut failure) => {
                group::identify::<A::Group, _>(&first_kill, first_name, &mut failure);
                let (error, text) = failure;
                return Err((error.map_init(AktorBranch::First), text));
            }
        };

        let mut second_context = context.clone();
        second_context.group.track_member();
        let second_kill = second_context.group.killswitch();
        let second_name = String::from(self.1.actor_name());
        let fallback = self.1.fallback();

        let (second, second_ready) = match self.1.launch(second_context) {
            Ok((handle, ready)) => (
                handle,
                A::Group::observe_ready(ready, second_kill.clone(), second_name),
            ),
            Err(mut failure) => {
                group::identify::<A::Group, _>(&second_kill, second_name, &mut failure);
                let (error, text) = failure;
                if error.init.is_some() {
                    return Err((error.map_init(AktorBranch::Second), text));
                }

                (
                    fallback(&context.group.fallback_member(Some(1))),
                    A::Group::failed_ready(error.reason, error.failure_id, text),
                )
            }
        };

        Ok((
            (first, second),
            A::Group::pair(first_ready, second_ready, first_kill, second_kill),
        ))
    }
}

fn begin_member<A: AktorStart>(
    setup: &A,
    group: &mut A::Group,
) -> Result<(), (AktorStartReason, String)> {
    let result = setup.begin(group);
    if let Err((_, text)) = &result {
        let kill = group.killswitch();
        if A::Group::startup_id(&kill).is_none() {
            A::Group::fail_startup(
                &kill,
                crate::AktorFailureInput::new(crate::AktorFailureSource::GroupDriver, text.clone()),
            );
        }
    }
    result
}

macro_rules! nested {
    ($last:ident) => { $last };
    ($first:ident, $($rest:ident),+) => { ($first, nested!($($rest),+)) };
}

// Pairs launch recursively. Give the caller the same flat positions as the handles.
macro_rules! branch {
    ($error:expr, $first:ident, $last:ident) => {
        match $error {
            AktorBranch::First(error) => AktorBranch::$first(error),
            AktorBranch::Second(error) => AktorBranch::$last(error),
            AktorBranch::Third(never)
            | AktorBranch::Fourth(never)
            | AktorBranch::Fifth(never)
            | AktorBranch::Sixth(never)
            | AktorBranch::Seventh(never)
            | AktorBranch::Eighth(never) => match never {},
        }
    };
    ($error:expr, $first:ident, $($rest:ident),+) => {
        match $error {
            AktorBranch::First(error) => AktorBranch::$first(error),
            AktorBranch::Second(error) => branch!(error, $($rest),+),
            AktorBranch::Third(never)
            | AktorBranch::Fourth(never)
            | AktorBranch::Fifth(never)
            | AktorBranch::Sixth(never)
            | AktorBranch::Seventh(never)
            | AktorBranch::Eighth(never) => match never {},
        }
    };
}

// The launch tree nests pairs to the right. Walk the same path for a refused handle.
macro_rules! fallback_handles {
    ($group:ident, $first:ident, $($next:ident),+) => {{
        let $first = $first(&$group.fallback_member(Some(0)));
        let group = $group.fallback_member(Some(1));
        fallback_handles!(@tail group, [$first], $($next),+)
    }};
    (@tail $group:ident, [$($done:ident),+], $last:ident) => {
        ($($done,)+ $last(&$group))
    };
    (@tail $group:ident, [$($done:ident),+], $next:ident, $($rest:ident),+) => {{
        let $next = $next(&$group.fallback_member(Some(0)));
        let group = $group.fallback_member(Some(1));
        fallback_handles!(@tail group, [$($done,)+ $next], $($rest),+)
    }};
}

macro_rules! tuple {
    ($first:ident: $first_value:ident: $first_case:ident, $($type:ident: $value:ident: $case:ident),+) => {
        impl<$first: AktorStart, $($type: AktorStart<Group = $first::Group>),+>
            AktorStart for ($first, $($type,)+)
        {
            type Group = $first::Group;
            type Error = AktorBranch<$first::Error, $($type::Error),+>;
            type Handles = ($first::Handles, $($type::Handles,)+);

            fn needs_tokio_group(&self) -> bool {
                let ($first_value, $($value,)+) = self;
                $first_value.needs_tokio_group() $(|| $value.needs_tokio_group())+
            }

            fn actor_name(&self) -> &str {
                self.0.actor_name()
            }

            fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
                let ($first_value, $($value,)+) = self;

                $(if $value.needs_tokio_group() {
                    return begin_member($value, group);
                })+

                begin_member($first_value, group)
            }

            fn closed(name: &str, group: &Self::Group) -> Self::Handles {
                ($first::closed(name, group), $($type::closed(name, group),)+)
            }

            fn fallback(&self) -> impl FnOnce(&Self::Group) -> Self::Handles + use<$first, $($type),+> {
                let ($first_value, $($value,)+) = self;
                let $first_value = $first_value.fallback();
                $(let $value = $value.fallback();)+

                move |group| fallback_handles!(group, $first_value, $($value),+)
            }

            fn launch(self, context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
                let ($first_value, $($value,)+) = self;
                let flat = |error: LaunchError<_>| {
                    error.map_init(|error| branch!(error, $first_case, $($case),+))
                };

                let (nested!($first_value, $($value),+), ready) =
                    match nested!($first_value, $($value),+).launch(context) {
                        Ok(launched) => launched,
                        Err((error, text)) => return Err((flat(error), text)),
                    };

                Ok((
                    ($first_value, $($value,)+),
                    $first::Group::map_ready(ready, flat),
                ))
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
