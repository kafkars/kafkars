//! Linear tracked `EndTxn` v3 call and causal coordinator invalidation.

mod terminal;

use std::{mem, time::Instant};

use kafka_driver::{Call, Driver, InvalidationDisposition, RouteFailureToken, RoutedCall};
use kafka_wire::EndTxnResponse;

use crate::protocol::transaction::{EndTxnDisposition, end_txn_v3_request};

use super::super::super::DriverOwner;
use super::{
    TransactionEndCallAdmissionFailure, TransactionEndCompletionFailureKind,
    failure::transaction_end_completion_failure,
};

pub(super) use terminal::TransactionEndTerminal;
pub(crate) use terminal::TransactionEndTerminalFact;
#[cfg(test)]
pub(super) use terminal::is_transaction_coordinator_route;

/// One accepted generated request retained until exactly one terminal.
#[must_use = "an accepted EndTxn call requires terminal settlement"]
pub(crate) struct TransactionEndCall {
    driver: Driver,
    state: TransactionEndState,
}

impl TransactionEndCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        disposition: EndTxnDisposition,
        deadline: Instant,
    ) -> Result<Self, TransactionEndCallAdmissionFailure> {
        let request =
            end_txn_v3_request(transactional_id, producer_id, producer_epoch, disposition);
        let call = driver
            .submit_tracked_transaction_end(transactional_id, request, deadline)
            .map_err(TransactionEndCallAdmissionFailure::Driver)?;
        Ok(Self {
            driver: driver.driver.clone(),
            state: TransactionEndState::Calling(call),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<TransactionEndTerminal, TransactionEndCompletionFailureKind>> {
        let state = mem::replace(&mut self.state, TransactionEndState::Consumed);
        match state {
            TransactionEndState::Calling(call) => {
                let Some(result) = call.try_result() else {
                    self.state = TransactionEndState::Calling(call);
                    return None;
                };
                drop(call);
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(error) => return Some(Err(transaction_end_completion_failure(error))),
                };
                let (result, selected_version, route_token) = outcome.into_parts();
                let mut terminal =
                    TransactionEndTerminal::new(selected_version, result, route_token);
                let Some(route_token) = terminal.take_failed_transaction_coordinator_route_token()
                else {
                    return Some(Ok(terminal));
                };
                self.poll_invalidation(terminal, TransactionEndInvalidation::Queued(route_token))
            }
            TransactionEndState::Invalidating {
                terminal,
                invalidation,
            } => self.poll_invalidation(terminal, invalidation),
            TransactionEndState::Consumed => None,
        }
    }

    /// Stops only a post-terminal coordinator invalidation at the public deadline.
    pub(crate) fn expire_refresh(&mut self) -> Option<TransactionEndTerminal> {
        match mem::replace(&mut self.state, TransactionEndState::Consumed) {
            TransactionEndState::Invalidating {
                terminal,
                invalidation,
            } => {
                drop(invalidation);
                Some(terminal)
            }
            state => {
                self.state = state;
                None
            }
        }
    }

    fn poll_invalidation(
        &mut self,
        mut terminal: TransactionEndTerminal,
        invalidation: TransactionEndInvalidation,
    ) -> Option<Result<TransactionEndTerminal, TransactionEndCompletionFailureKind>> {
        let invalidation = match invalidation {
            TransactionEndInvalidation::Queued(route_token) => {
                match self.driver.invalidate(route_token) {
                    Ok(call) => TransactionEndInvalidation::Active(call),
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        TransactionEndInvalidation::Queued(route_token)
                    }
                }
            }
            TransactionEndInvalidation::Active(call) => {
                if let Some(result) = call.try_result() {
                    if matches!(
                        result,
                        Ok(InvalidationDisposition::Applied
                            | InvalidationDisposition::IgnoredStale)
                    ) {
                        terminal.mark_coordinator_refresh_completed();
                    }
                    drop(call);
                    return Some(Ok(terminal));
                }
                TransactionEndInvalidation::Active(call)
            }
        };
        self.state = TransactionEndState::Invalidating {
            terminal,
            invalidation,
        };
        None
    }

    pub(crate) fn discard_after_driver_shutdown(self) {
        drop(self);
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "the linear state retains the exact EndTxn terminal until invalidation settles"
)]
enum TransactionEndState {
    Calling(RoutedCall<EndTxnResponse>),
    Invalidating {
        terminal: TransactionEndTerminal,
        invalidation: TransactionEndInvalidation,
    },
    Consumed,
}

enum TransactionEndInvalidation {
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}
