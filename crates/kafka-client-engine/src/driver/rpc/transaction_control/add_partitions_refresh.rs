//! Causal transaction-coordinator invalidation before `AddPartitions` settlement.

use std::mem;

use kafka_driver::{
    Call, CompletionError, Driver, InvalidationDisposition, RouteFailureToken, RoutedCall,
};
use kafka_wire::AddPartitionsToTxnResponse;

use super::add_partitions::{TransactionAddPartitionsTerminal, TransactionPartitionTarget};

#[expect(
    clippy::large_enum_variant,
    reason = "the linear state retains exact raw terminal and route-refresh ownership inline"
)]
pub(super) enum TransactionAddPartitionsCallState {
    Calling {
        call: RoutedCall<AddPartitionsToTxnResponse>,
        targets: Vec<TransactionPartitionTarget>,
    },
    Refreshing {
        terminal: TransactionAddPartitionsTerminal,
        refresh: TransactionCoordinatorRefresh,
    },
    Consumed,
}

impl TransactionAddPartitionsCallState {
    pub(super) const fn calling(
        call: RoutedCall<AddPartitionsToTxnResponse>,
        targets: Vec<TransactionPartitionTarget>,
    ) -> Self {
        Self::Calling { call, targets }
    }

    pub(super) fn poll(&mut self, driver: &Driver) -> TransactionAddPartitionsPoll {
        let state = mem::replace(self, Self::Consumed);
        match state {
            Self::Calling { call, targets } => {
                let Some(result) = call.try_result() else {
                    *self = Self::Calling { call, targets };
                    return TransactionAddPartitionsPoll::Pending;
                };
                drop(call);
                let mut terminal = match result {
                    Ok(outcome) => {
                        let (result, selected_version, route_token) = outcome.into_parts();
                        TransactionAddPartitionsTerminal::new(
                            selected_version,
                            result,
                            route_token,
                            targets,
                        )
                    }
                    Err(error) => return TransactionAddPartitionsPoll::Terminal(Err(error)),
                };
                let Some(token) = terminal.take_transaction_coordinator_refresh_token() else {
                    return TransactionAddPartitionsPoll::Terminal(Ok(terminal));
                };
                self.poll_refresh(
                    driver,
                    terminal,
                    TransactionCoordinatorRefresh::Queued(token),
                )
            }
            Self::Refreshing { terminal, refresh } => self.poll_refresh(driver, terminal, refresh),
            Self::Consumed => TransactionAddPartitionsPoll::Pending,
        }
    }

    fn poll_refresh(
        &mut self,
        driver: &Driver,
        mut terminal: TransactionAddPartitionsTerminal,
        refresh: TransactionCoordinatorRefresh,
    ) -> TransactionAddPartitionsPoll {
        match poll_coordinator_refresh(driver, refresh) {
            RefreshPoll::Ready { crossed_barrier } => {
                if crossed_barrier {
                    terminal.mark_coordinator_refresh_completed();
                }
                TransactionAddPartitionsPoll::Terminal(Ok(terminal))
            }
            RefreshPoll::Submitted(refresh) => {
                *self = Self::Refreshing { terminal, refresh };
                TransactionAddPartitionsPoll::Progress
            }
            RefreshPoll::Pending(refresh) => {
                *self = Self::Refreshing { terminal, refresh };
                TransactionAddPartitionsPoll::Pending
            }
        }
    }

    pub(super) fn discard_after_driver_shutdown(self) {
        match self {
            Self::Calling { call, .. } => drop(call),
            Self::Refreshing { terminal, refresh } => {
                terminal.discard();
                drop(refresh);
            }
            Self::Consumed => {}
        }
    }
}

impl super::add_partitions::TransactionAddPartitionsCall {
    /// Stops only a post-terminal coordinator refresh at the public deadline.
    pub(crate) fn expire_refresh(&mut self) -> Option<TransactionAddPartitionsTerminal> {
        match mem::replace(&mut self.state, TransactionAddPartitionsCallState::Consumed) {
            TransactionAddPartitionsCallState::Refreshing { terminal, refresh } => {
                drop(refresh);
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

enum RefreshPoll {
    Ready { crossed_barrier: bool },
    Submitted(TransactionCoordinatorRefresh),
    Pending(TransactionCoordinatorRefresh),
}

fn poll_coordinator_refresh(
    driver: &Driver,
    refresh: TransactionCoordinatorRefresh,
) -> RefreshPoll {
    match refresh {
        TransactionCoordinatorRefresh::Queued(token) => match driver.invalidate(token) {
            Ok(call) => RefreshPoll::Submitted(TransactionCoordinatorRefresh::Active(call)),
            Err(rejection) => {
                let (_source, token) = rejection.into_parts();
                RefreshPoll::Pending(TransactionCoordinatorRefresh::Queued(token))
            }
        },
        TransactionCoordinatorRefresh::Active(call) => match call.try_result() {
            Some(result) => RefreshPoll::Ready {
                crossed_barrier: matches!(
                    result,
                    Ok(InvalidationDisposition::Applied | InvalidationDisposition::IgnoredStale)
                ),
            },
            None => RefreshPoll::Pending(TransactionCoordinatorRefresh::Active(call)),
        },
    }
}

/// One bounded poll of an accepted call and its causal coordinator refresh.
#[expect(
    clippy::large_enum_variant,
    reason = "the poll transfers exact terminal authority inline without hidden allocation"
)]
pub(crate) enum TransactionAddPartitionsPoll {
    Pending,
    Progress,
    Terminal(Result<TransactionAddPartitionsTerminal, CompletionError>),
}
