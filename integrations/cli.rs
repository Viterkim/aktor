use std::{io, process::ExitCode};

pub struct Report(pub String);
impl<E> From<(E, String)> for Report {
    fn from((_, report): (E, String)) -> Self {
        Self(report)
    }
}
impl From<aktor::AktorDeliveryError> for Report {
    fn from(error: aktor::AktorDeliveryError) -> Self {
        Self(error.report)
    }
}
impl From<io::Error> for Report {
    fn from(error: io::Error) -> Self {
        Self(error.to_string())
    }
}
impl From<&str> for Report {
    fn from(message: &str) -> Self {
        Self(message.to_owned())
    }
}

pub fn exit(result: Result<(), Report>) -> ExitCode {
    if let Err(Report(report)) = result {
        eprintln!("{report}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
