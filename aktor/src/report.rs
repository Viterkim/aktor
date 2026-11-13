use crate::{AktorError, AktorFailure, AktorGroupError};
use alloc::string::{String, ToString};
use core::fmt;

impl From<AktorGroupError> for (AktorGroupError, String) {
    fn from(error: AktorGroupError) -> Self {
        let report = error.diagnostics();

        (error, report)
    }
}

impl AktorGroupError {
    pub(crate) fn diagnostics(&self) -> String {
        match self {
            Self::StartFailed { report } => report.to_string(),
            error => error.to_string(),
        }
    }
}

impl From<AktorError> for AktorFailure {
    fn from(error: AktorError) -> Self {
        Self {
            diagnostics: error.diagnostics,
            primary: false,
        }
    }
}

impl<T> AktorError<T> {
    #[doc(hidden)]
    pub fn fmt_display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        with_top(f, "actor lifecycle failed", &self.diagnostics)
    }
}

impl AktorFailure {
    #[doc(hidden)]
    pub fn fmt_display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        with_top(f, "actor lifecycle failed", &self.diagnostics)
    }
}

pub fn with_top(f: &mut fmt::Formatter<'_>, message: &str, diagnostics: &str) -> fmt::Result {
    match diagnostics.lines().next() {
        Some(top) if !top.is_empty() => write!(f, "{message}: {top}"),
        _ => f.write_str(message),
    }
}

pub fn diagnostics(error: &(dyn core::error::Error + 'static)) -> String {
    render_diagnostic(|| {
        let mut output = String::new();
        let mut source = Some(error);

        for _ in 0..64 {
            let Some(error) = source else {
                return output;
            };
            if !output.is_empty() {
                output.push_str("\ncaused by: ");
            }
            output.push_str(&alloc::format!("{error:#}"));
            source = error.source();
        }

        if source.is_some() {
            output.push_str("\ncaused by: ... (source chain truncated)");
        }
        output
    })
}

pub(crate) fn tree_report<E: core::error::Error + 'static>(tree: &er::ErTree<E>) -> String {
    render_diagnostic(|| tree.er_report())
}

pub fn render_diagnostic(render: impl FnOnce() -> String) -> String {
    #[cfg(feature = "std")]
    {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(render)) {
            Ok(diagnostics) => diagnostics,
            Err(payload) => {
                let message = alloc::format!(
                    "error formatter panicked: {}",
                    crate::panic::panic_message(&payload)
                );

                crate::panic::dispose_secondary(payload);
                message
            }
        }
    }
    #[cfg(not(feature = "std"))]
    {
        render()
    }
}
