//! Lossless translation from hosted execution ownership into public terminal facts.

use kafka_client_core::DeliveryStatus;

use crate::{
    consumer::{
        share::{ShareAcknowledgementExecutionFailureKind, ShareAcknowledgementExecutionOutcome},
        share_batch::ShareAcknowledgementRecovery,
    },
    driver::share_acknowledge::{
        ShareAcknowledgeDriverFailureKind, ShareAcknowledgeDriverSubmitErrorKind,
        ShareAcknowledgeResolution,
    },
};

use super::outcome::{
    ShareAcknowledgeBrokerError, ShareAcknowledgeDeliveryStatus, ShareAcknowledgeFailure,
    ShareAcknowledgeFailureKind, ShareAcknowledgeOutcome, ShareAcknowledgeResponse,
};

pub(in crate::consumer) fn public_outcome(
    outcome: ShareAcknowledgementExecutionOutcome,
    recovery: ShareAcknowledgementRecovery,
) -> ShareAcknowledgeOutcome {
    match outcome {
        ShareAcknowledgementExecutionOutcome::Responded(ShareAcknowledgeResolution::Succeeded(
            response,
        )) => ShareAcknowledgeOutcome::Responded(ShareAcknowledgeResponse(response)),
        ShareAcknowledgementExecutionOutcome::Responded(
            ShareAcknowledgeResolution::BrokerRejected(rejection),
        ) => ShareAcknowledgeOutcome::Failed(ShareAcknowledgeFailure {
            kind: ShareAcknowledgeFailureKind::BrokerRejected,
            delivery: ShareAcknowledgeDeliveryStatus::PossiblySent,
            broker: Some(ShareAcknowledgeBrokerError {
                throttle_time_ms: rejection.throttle_time_ms,
                broker_code: rejection.error_code.get(),
                message: rejection.error_message,
            }),
            retry: None,
        }),
        ShareAcknowledgementExecutionOutcome::Responded(ShareAcknowledgeResolution::Failed {
            kind,
            delivery,
        }) => execution_failure(
            ShareAcknowledgementExecutionFailureKind::Driver(kind),
            delivery,
            None,
            recovery,
        ),
        ShareAcknowledgementExecutionOutcome::Failed {
            kind,
            delivery,
            retry,
        } => execution_failure(kind, delivery, retry, recovery),
    }
}

fn execution_failure(
    kind: ShareAcknowledgementExecutionFailureKind,
    delivery: DeliveryStatus,
    retry: Option<kafka_client_core::ShareAcknowledgement>,
    recovery: ShareAcknowledgementRecovery,
) -> ShareAcknowledgeOutcome {
    ShareAcknowledgeOutcome::Failed(ShareAcknowledgeFailure {
        kind: public_failure_kind(kind),
        delivery: public_delivery(delivery),
        broker: None,
        retry: retry.map(|acknowledgement| recovery.recover(Box::new(acknowledgement))),
    })
}

const fn public_delivery(delivery: DeliveryStatus) -> ShareAcknowledgeDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => ShareAcknowledgeDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => ShareAcknowledgeDeliveryStatus::PossiblySent,
    }
}

const fn public_failure_kind(
    kind: ShareAcknowledgementExecutionFailureKind,
) -> ShareAcknowledgeFailureKind {
    match kind {
        ShareAcknowledgementExecutionFailureKind::Submit(submit) => submit_failure(submit),
        ShareAcknowledgementExecutionFailureKind::Driver(driver) => driver_failure(driver),
        ShareAcknowledgementExecutionFailureKind::BrokerMismatch => {
            ShareAcknowledgeFailureKind::InvalidResponse
        }
        ShareAcknowledgementExecutionFailureKind::Completion(_)
        | ShareAcknowledgementExecutionFailureKind::Core(_)
        | ShareAcknowledgementExecutionFailureKind::Preparation(_) => {
            ShareAcknowledgeFailureKind::Internal
        }
    }
}

const fn submit_failure(
    kind: ShareAcknowledgeDriverSubmitErrorKind,
) -> ShareAcknowledgeFailureKind {
    match kind {
        ShareAcknowledgeDriverSubmitErrorKind::Full
        | ShareAcknowledgeDriverSubmitErrorKind::Terminal => {
            ShareAcknowledgeFailureKind::DriverRejected
        }
    }
}

const fn driver_failure(kind: ShareAcknowledgeDriverFailureKind) -> ShareAcknowledgeFailureKind {
    match kind {
        ShareAcknowledgeDriverFailureKind::DeadlineElapsed => {
            ShareAcknowledgeFailureKind::DeadlineElapsed
        }
        ShareAcknowledgeDriverFailureKind::Compatibility => {
            ShareAcknowledgeFailureKind::Compatibility
        }
        ShareAcknowledgeDriverFailureKind::DriverRejected => {
            ShareAcknowledgeFailureKind::DriverRejected
        }
        ShareAcknowledgeDriverFailureKind::Transport => ShareAcknowledgeFailureKind::Transport,
        ShareAcknowledgeDriverFailureKind::InvalidResponse => {
            ShareAcknowledgeFailureKind::InvalidResponse
        }
        ShareAcknowledgeDriverFailureKind::ResponseTooLarge => {
            ShareAcknowledgeFailureKind::ResponseTooLarge
        }
    }
}
