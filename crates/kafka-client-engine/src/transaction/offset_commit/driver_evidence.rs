//! Tracked terminal evidence normalization and explicit route discard.

use kafka_client_core::{
    DeliveryStatus, TransactionOffsetCommitConsequence, TransactionOffsetCommitStage,
};

use crate::{
    driver::transaction_offsets::{
        TransactionAddOffsetsTerminal, TransactionAddOffsetsTerminalFact,
        TransactionOffsetCommitTerminal, TransactionOffsetCommitTerminalFact,
        TransactionOffsetDriverFailureKind,
    },
    protocol::transaction::{
        AddOffsetsToTxnOutcome, TransactionBrokerCategory, TransactionOffsetCommitOutcome,
    },
};

use super::{
    model::TransactionOffsetCommitFailureKind,
    port::{TransactionOffsetCommitPortEvidence, TransactionOffsetCommitPortFact},
};

pub(super) type Correlation = (
    kafka_client_core::TransactionEpoch,
    kafka_client_core::TransactionOffsetCommitId,
    TransactionOffsetCommitStage,
);

enum DriverTransactionOffsetCommitEvidence {
    Add {
        correlation: Correlation,
        terminal: TransactionAddOffsetsTerminal,
    },
    Commit {
        correlation: Correlation,
        terminal: TransactionOffsetCommitTerminal,
    },
    Failed {
        correlation: Correlation,
        fact: TransactionOffsetCommitPortFact,
    },
}

pub(super) fn add(
    correlation: Correlation,
    terminal: TransactionAddOffsetsTerminal,
) -> Box<dyn TransactionOffsetCommitPortEvidence> {
    Box::new(DriverTransactionOffsetCommitEvidence::Add {
        correlation,
        terminal,
    })
}

pub(super) fn commit(
    correlation: Correlation,
    terminal: TransactionOffsetCommitTerminal,
) -> Box<dyn TransactionOffsetCommitPortEvidence> {
    Box::new(DriverTransactionOffsetCommitEvidence::Commit {
        correlation,
        terminal,
    })
}

pub(super) fn closed(correlation: Correlation) -> Box<dyn TransactionOffsetCommitPortEvidence> {
    Box::new(DriverTransactionOffsetCommitEvidence::Failed {
        correlation,
        fact: failed(
            TransactionOffsetCommitConsequence::AbortRequired,
            TransactionOffsetCommitFailureKind::DriverShutdown,
            DeliveryStatus::PossiblySent,
        ),
    })
}

impl TransactionOffsetCommitPortEvidence for DriverTransactionOffsetCommitEvidence {
    fn correlation(&self) -> Correlation {
        match self {
            Self::Add { correlation, .. }
            | Self::Commit { correlation, .. }
            | Self::Failed { correlation, .. } => *correlation,
        }
    }

    fn fact(&self) -> TransactionOffsetCommitPortFact {
        match self {
            Self::Add { terminal, .. } => add_offsets_fact(terminal),
            Self::Commit { terminal, .. } => offset_commit_fact(terminal),
            Self::Failed { fact, .. } => *fact,
        }
    }

    fn discard(self: Box<Self>) {
        match *self {
            Self::Add { terminal, .. } => terminal.discard(),
            Self::Commit { terminal, .. } => terminal.discard(),
            Self::Failed { .. } => {}
        }
    }
}

fn add_offsets_fact(terminal: &TransactionAddOffsetsTerminal) -> TransactionOffsetCommitPortFact {
    let retry_safe_after_refresh = terminal.retry_safe_after_refresh();
    match terminal.fact() {
        TransactionAddOffsetsTerminalFact::Response(Ok(AddOffsetsToTxnOutcome::Added {
            ..
        })) => TransactionOffsetCommitPortFact::Succeeded,
        TransactionAddOffsetsTerminalFact::Response(Ok(AddOffsetsToTxnOutcome::Rejected {
            error,
            ..
        })) if retry_safe_after_refresh => retryable_coordinator_loss(
            TransactionOffsetCommitFailureKind::Broker {
                code: error.code().get(),
                fenced: false,
            },
            DeliveryStatus::PossiblySent,
        ),
        TransactionAddOffsetsTerminalFact::Response(Ok(AddOffsetsToTxnOutcome::Rejected {
            error,
            ..
        })) => broker_failure(error.code().get(), error.category()),
        TransactionAddOffsetsTerminalFact::Response(Err(_)) => invalid_response(),
        TransactionAddOffsetsTerminalFact::Failed { kind, delivery } => {
            let failure = driver_failure_kind(kind);
            if retry_safe_after_refresh {
                retryable_coordinator_loss(failure, delivery)
            } else {
                failed(
                    TransactionOffsetCommitConsequence::AbortRequired,
                    failure,
                    delivery,
                )
            }
        }
    }
}

fn offset_commit_fact(
    terminal: &TransactionOffsetCommitTerminal,
) -> TransactionOffsetCommitPortFact {
    let retry_safe_after_refresh = terminal.retry_safe_after_refresh();
    match terminal.fact() {
        TransactionOffsetCommitTerminalFact::Response(Ok(response)) => {
            let mut rejection = None;
            for result in response.offsets() {
                let TransactionOffsetCommitOutcome::Rejected(error) = result.outcome() else {
                    continue;
                };
                let failure = broker_failure(error.code().get(), error.category());
                if error.category() == TransactionBrokerCategory::Fenced {
                    return failure;
                }
                rejection.get_or_insert((error.code().get(), failure));
            }
            match rejection {
                Some((code, _failure)) if retry_safe_after_refresh => retryable_coordinator_loss(
                    TransactionOffsetCommitFailureKind::Broker {
                        code,
                        fenced: false,
                    },
                    DeliveryStatus::PossiblySent,
                ),
                Some((_code, failure)) => failure,
                None => TransactionOffsetCommitPortFact::Succeeded,
            }
        }
        TransactionOffsetCommitTerminalFact::Response(Err(_)) => invalid_response(),
        TransactionOffsetCommitTerminalFact::Failed { kind, delivery } => {
            driver_failure(kind, delivery)
        }
    }
}

fn broker_failure(
    code: i16,
    category: TransactionBrokerCategory,
) -> TransactionOffsetCommitPortFact {
    failed(
        if category == TransactionBrokerCategory::Fenced {
            TransactionOffsetCommitConsequence::Fatal
        } else {
            TransactionOffsetCommitConsequence::AbortRequired
        },
        TransactionOffsetCommitFailureKind::Broker {
            code,
            fenced: category == TransactionBrokerCategory::Fenced,
        },
        DeliveryStatus::PossiblySent,
    )
}

fn driver_failure(
    kind: TransactionOffsetDriverFailureKind,
    delivery: DeliveryStatus,
) -> TransactionOffsetCommitPortFact {
    let consequence = if kind == TransactionOffsetDriverFailureKind::InvalidResponse {
        TransactionOffsetCommitConsequence::Fatal
    } else {
        TransactionOffsetCommitConsequence::AbortRequired
    };
    failed(consequence, driver_failure_kind(kind), delivery)
}

const fn driver_failure_kind(
    kind: TransactionOffsetDriverFailureKind,
) -> TransactionOffsetCommitFailureKind {
    match kind {
        TransactionOffsetDriverFailureKind::DeadlineElapsed => {
            TransactionOffsetCommitFailureKind::DeadlineElapsed
        }
        TransactionOffsetDriverFailureKind::Compatibility => {
            TransactionOffsetCommitFailureKind::Compatibility
        }
        TransactionOffsetDriverFailureKind::InvalidResponse => {
            TransactionOffsetCommitFailureKind::InvalidResponse
        }
        TransactionOffsetDriverFailureKind::Transport => {
            TransactionOffsetCommitFailureKind::Transport
        }
    }
}

fn invalid_response() -> TransactionOffsetCommitPortFact {
    failed(
        TransactionOffsetCommitConsequence::Fatal,
        TransactionOffsetCommitFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    )
}

const fn failed(
    consequence: TransactionOffsetCommitConsequence,
    kind: TransactionOffsetCommitFailureKind,
    delivery: DeliveryStatus,
) -> TransactionOffsetCommitPortFact {
    TransactionOffsetCommitPortFact::Failed {
        consequence,
        kind,
        delivery,
    }
}

const fn retryable_coordinator_loss(
    kind: TransactionOffsetCommitFailureKind,
    delivery: DeliveryStatus,
) -> TransactionOffsetCommitPortFact {
    TransactionOffsetCommitPortFact::RetryableCoordinatorLoss { kind, delivery }
}
