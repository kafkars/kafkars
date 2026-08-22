//! Concrete tracked-call adapter for transaction partition enrollment.

use std::sync::Arc;

use kafka_client_core::{DeliveryStatus, Moment, TransactionEpoch};

use crate::{
    driver::{
        DriverOwner,
        transaction_control::{
            TransactionAddPartitionsCall, TransactionAddPartitionsTerminal,
            TransactionAddPartitionsTerminalFact, TransactionControlDriverFailureKind,
            TransactionPartitionTarget,
        },
    },
    protocol::transaction::{AddPartitionsToTxnPartitionOutcome, TransactionBrokerCategory},
};

use super::{
    host::TransactionPartitionEnrollmentOwner,
    model::{TransactionPartitionEnrollmentFailureKind, TransactionPartitionEnrollmentTurn},
    port::{
        TransactionPartitionEnrollmentPort, TransactionPartitionEnrollmentPortCall,
        TransactionPartitionEnrollmentPortCallPoll, TransactionPartitionEnrollmentPortEvidence,
        TransactionPartitionEnrollmentPortFact, TransactionPartitionEnrollmentRequest,
    },
};

impl TransactionPartitionEnrollmentOwner {
    /// Performs at most one enrollment action through the concrete driver.
    pub(crate) fn turn(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> TransactionPartitionEnrollmentTurn {
        self.turn_with(
            now,
            &mut DriverTransactionPartitionEnrollmentPort { driver },
        )
    }
}

struct DriverTransactionPartitionEnrollmentPort<'a> {
    driver: &'a DriverOwner,
}

impl TransactionPartitionEnrollmentPort for DriverTransactionPartitionEnrollmentPort<'_> {
    fn submit(
        &mut self,
        request: TransactionPartitionEnrollmentRequest<'_>,
    ) -> Result<Box<dyn TransactionPartitionEnrollmentPortCall>, ()> {
        TransactionAddPartitionsCall::submit(
            self.driver,
            request.transactional_id,
            request.producer_id,
            request.producer_epoch,
            vec![TransactionPartitionTarget::new(
                Arc::clone(request.topic),
                request.partition,
            )],
            request.deadline,
        )
        .map(|call| {
            Box::new(DriverTransactionPartitionEnrollmentCall {
                epoch: request.epoch,
                call,
            }) as Box<dyn TransactionPartitionEnrollmentPortCall>
        })
        .map_err(|_error| ())
    }
}

struct DriverTransactionPartitionEnrollmentCall {
    epoch: TransactionEpoch,
    call: TransactionAddPartitionsCall,
}

impl TransactionPartitionEnrollmentPortCall for DriverTransactionPartitionEnrollmentCall {
    fn poll(&mut self, deadline_elapsed: bool) -> TransactionPartitionEnrollmentPortCallPoll {
        if deadline_elapsed {
            if let Some(terminal) = self.call.expire_refresh() {
                return TransactionPartitionEnrollmentPortCallPoll::DeadlineElapsed(Box::new(
                    DriverTransactionPartitionEnrollmentEvidence::Tracked {
                        epoch: self.epoch,
                        terminal,
                    },
                ));
            }
        }
        let terminal = match self.call.poll() {
            crate::driver::transaction_control::TransactionAddPartitionsPoll::Pending => {
                return TransactionPartitionEnrollmentPortCallPoll::Pending;
            }
            crate::driver::transaction_control::TransactionAddPartitionsPoll::Progress => {
                return TransactionPartitionEnrollmentPortCallPoll::Progress;
            }
            crate::driver::transaction_control::TransactionAddPartitionsPoll::Terminal(Ok(
                terminal,
            )) => DriverTransactionPartitionEnrollmentEvidence::Tracked {
                epoch: self.epoch,
                terminal,
            },
            crate::driver::transaction_control::TransactionAddPartitionsPoll::Terminal(Err(
                _completion_error,
            )) => DriverTransactionPartitionEnrollmentEvidence::Closed { epoch: self.epoch },
        };
        TransactionPartitionEnrollmentPortCallPoll::Terminal(Box::new(terminal))
    }

    fn discard_after_driver_shutdown(self: Box<Self>) {
        self.call.discard_after_driver_shutdown();
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "terminal evidence retains the exact driver owner inline through settlement"
)]
enum DriverTransactionPartitionEnrollmentEvidence {
    Tracked {
        epoch: TransactionEpoch,
        terminal: TransactionAddPartitionsTerminal,
    },
    Closed {
        epoch: TransactionEpoch,
    },
}

impl TransactionPartitionEnrollmentPortEvidence for DriverTransactionPartitionEnrollmentEvidence {
    fn epoch(&self) -> TransactionEpoch {
        match self {
            Self::Tracked { epoch, .. } | Self::Closed { epoch } => *epoch,
        }
    }

    fn fact(&self) -> TransactionPartitionEnrollmentPortFact {
        let Self::Tracked { terminal, .. } = self else {
            return failed(
                TransactionPartitionEnrollmentFailureKind::DriverClosed,
                DeliveryStatus::PossiblySent,
            );
        };
        match terminal.fact() {
            TransactionAddPartitionsTerminalFact::Response(Ok(response)) => {
                normalized_response_fact(response.partitions(), terminal.retry_safe_after_refresh())
            }
            TransactionAddPartitionsTerminalFact::Response(Err(_failure)) => failed(
                TransactionPartitionEnrollmentFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
            TransactionAddPartitionsTerminalFact::Failed { kind, delivery } => {
                let kind = driver_failure(kind);
                if terminal.retry_safe_after_refresh() {
                    retryable_coordinator_loss(kind, delivery)
                } else {
                    failed(kind, delivery)
                }
            }
        }
    }

    fn discard(self: Box<Self>) {
        if let Self::Tracked { terminal, .. } = *self {
            terminal.discard();
        }
    }
}

fn normalized_response_fact(
    partitions: &[crate::protocol::transaction::AddPartitionsToTxnPartitionResultRef<'_>],
    retry_safe_after_refresh: bool,
) -> TransactionPartitionEnrollmentPortFact {
    let mut rejection = None;
    for partition in partitions {
        let AddPartitionsToTxnPartitionOutcome::Rejected(error) = partition.outcome() else {
            continue;
        };
        let kind = TransactionPartitionEnrollmentFailureKind::Broker {
            code: error.code().get(),
            fenced: error.category() == TransactionBrokerCategory::Fenced,
        };
        if kind.is_fatal() {
            return failed(kind, DeliveryStatus::PossiblySent);
        }
        rejection.get_or_insert(kind);
    }
    rejection.map_or(TransactionPartitionEnrollmentPortFact::Enrolled, |kind| {
        TransactionPartitionEnrollmentPortFact::broker_rejection(kind, retry_safe_after_refresh)
    })
}

const fn driver_failure(
    kind: TransactionControlDriverFailureKind,
) -> TransactionPartitionEnrollmentFailureKind {
    match kind {
        TransactionControlDriverFailureKind::DeadlineElapsed => {
            TransactionPartitionEnrollmentFailureKind::DeadlineElapsed
        }
        TransactionControlDriverFailureKind::Compatibility => {
            TransactionPartitionEnrollmentFailureKind::Compatibility
        }
        TransactionControlDriverFailureKind::InvalidResponse => {
            TransactionPartitionEnrollmentFailureKind::InvalidResponse
        }
        TransactionControlDriverFailureKind::Transport => {
            TransactionPartitionEnrollmentFailureKind::Transport
        }
    }
}

const fn failed(
    kind: TransactionPartitionEnrollmentFailureKind,
    delivery: DeliveryStatus,
) -> TransactionPartitionEnrollmentPortFact {
    TransactionPartitionEnrollmentPortFact::Failed { kind, delivery }
}

const fn retryable_coordinator_loss(
    kind: TransactionPartitionEnrollmentFailureKind,
    delivery: DeliveryStatus,
) -> TransactionPartitionEnrollmentPortFact {
    TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss { kind, delivery }
}
