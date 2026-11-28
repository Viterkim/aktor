use super::*;

#[derive(Er)]
pub struct EncodeAnswerErr;
pub fn encode(
    mut answer: Message<'_>,
    limit: usize,
    errors: &Failures,
) -> ErResult<(Vec<u8>, Option<aktor::FailureId>), EncodeAnswerErr> {
    let error = match wire::encode(&answer, limit) {
        Ok(bytes) => {
            let failure = match &answer {
                Message::Reject(error)
                | Message::Answer {
                    output: Err(error), ..
                }
                | Message::LatestAnswer {
                    output: Err(error), ..
                } => Some(error),
                _ => None,
            };
            let receipt = failure
                .filter(|f| f.owner_report.is_some())
                .and_then(|f| f.failure_id);
            return Ok((bytes, receipt));
        }
        Err(error) => error,
    };
    let reason = match error.top {
        wire::EncodeFrameErr::TooLarge { size, limit } => Rejection::ReplyTooLarge {
            size: size as u64,
            limit: limit as u32,
        },
        wire::EncodeFrameErr::Encode => Rejection::ReplyEncoding,
    };

    // Even MIN_FRAME can carry the reason and the owner's failure ID.
    let failure = match &mut answer {
        Message::Answer { output, .. } | Message::LatestAnswer { output, .. } => match output {
            Err(failure) => Some(failure),
            Ok(_) => {
                *output = Err(WireFailure::rejected(
                    AktorDeliveryOutcome::OutcomeUnknown,
                    Cause::Codec,
                    reason,
                ));
                None
            }
        },
        Message::Reject(failure) => Some(failure),
        _ => er_bail!(EncodeAnswerErr, error),
    };

    let error = if let Some(failure) = failure {
        let reason = if failure.failure_id.is_none() && failure.owner_report.is_some() {
            failure.reason
        } else {
            reason
        };
        let replacement = WireFailure {
            reason,
            owner_report: None,
            ..*failure
        };
        let original = std::mem::replace(failure, replacement);
        error.er_add(original.received())
    } else {
        error
    };
    errors.record(er_new!(ServerFailure::session(Cause::Codec), error));

    wire::encode(&answer, limit)
        .map(|bytes| (bytes, None))
        .er(())
}
