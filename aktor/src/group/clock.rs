use std::time::Duration;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(
    inline_js = "export function now() { return performance.now(); }\nexport function timeOrigin() { return performance.timeOrigin; }"
)]
extern "C" {
    fn now() -> f64;

    #[wasm_bindgen(js_name = timeOrigin)]
    fn time_origin() -> f64;
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(Duration);
impl Instant {
    pub fn now() -> Self {
        Self(
            Duration::from_secs_f64(time_origin() / 1000.0)
                + Duration::from_secs_f64(now() / 1000.0),
        )
    }

    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }

    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        self.0.checked_sub(duration).map(Self)
    }

    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}
