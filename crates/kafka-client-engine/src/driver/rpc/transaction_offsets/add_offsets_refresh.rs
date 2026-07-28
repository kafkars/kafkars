//! Causal transaction-coordinator refresh barrier for `AddOffsetsToTxn`.

use std::mem;

use kafka_driver::{Call, Driver, InvalidationDisposition, RouteFailureToken};

use super::add_offsets::{
    TransactionAddOffsetsCall, TransactionAddOffsetsState, TransactionAddOffsetsTerminal,
};

impl TransactionAddOffsetsCall {
    pub(crate) fn expire_refresh(&mut self) -> Option<TransactionAddOffsetsTerminal> {
        match mem::replace(&mut self.state, TransactionAddOffsetsState::Consumed) {
            TransactionAddOffsetsState::Refreshing {
                terminal,
                coordinator_refresh,
            } => {
                drop(coordinator_refresh);
                Some(terminal)
            }
            state => {
                self.state = state;
                None
            }
        }
    }
}

pub(super) enum TransactionCoordinatorRefresh {
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionCoordinatorRefreshPoll {
    Ready { crossed_barrier: bool },
    Submitted,
    Pending,
}

pub(super) fn poll_coordinator_refresh(
    driver: &Driver,
    coordinator_refresh: TransactionCoordinatorRefresh,
) -> (
    TransactionCoordinatorRefreshPoll,
    TransactionCoordinatorRefresh,
) {
    match coordinator_refresh {
        TransactionCoordinatorRefresh::Queued(route_token) => {
            match driver.invalidate(route_token) {
                Ok(call) => (
                    TransactionCoordinatorRefreshPoll::Submitted,
                    TransactionCoordinatorRefresh::Active(call),
                ),
                Err(rejection) => {
                    let (_source, route_token) = rejection.into_parts();
                    (
                        TransactionCoordinatorRefreshPoll::Pending,
                        TransactionCoordinatorRefresh::Queued(route_token),
                    )
                }
            }
        }
        TransactionCoordinatorRefresh::Active(call) => match call.try_result() {
            Some(result) => (
                TransactionCoordinatorRefreshPoll::Ready {
                    crossed_barrier: matches!(
                        result,
                        Ok(InvalidationDisposition::Applied
                            | InvalidationDisposition::IgnoredStale)
                    ),
                },
                TransactionCoordinatorRefresh::Active(call),
            ),
            None => (
                TransactionCoordinatorRefreshPoll::Pending,
                TransactionCoordinatorRefresh::Active(call),
            ),
        },
    }
}
