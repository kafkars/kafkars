//! Causal group-coordinator invalidation before `TxnOffsetCommit` settlement.

use std::mem;

use kafka_driver::{
    Call, CompletionError, Driver, InvalidationDisposition, RouteFailureToken, RoutedCall,
};
use kafka_wire::TxnOffsetCommitResponse;

use super::{
    offset_commit::{
        RecoveredTransactionOffsetCommitCall, TransactionOffsetCommitCall,
        TransactionOffsetCommitTerminal,
    },
    offset_commit_target::TransactionOffsetCommitTarget,
};

#[expect(
    clippy::large_enum_variant,
    reason = "the linear state retains exact raw terminal and route-refresh ownership inline"
)]
pub(super) enum TransactionOffsetCommitCallState {
    Calling {
        call: RoutedCall<TxnOffsetCommitResponse>,
        targets: Vec<TransactionOffsetCommitTarget>,
    },
    Refreshing {
        terminal: TransactionOffsetCommitTerminal,
        refresh: GroupCoordinatorRefresh,
    },
    Consumed,
}

impl TransactionOffsetCommitCallState {
    pub(super) const fn calling(
        call: RoutedCall<TxnOffsetCommitResponse>,
        targets: Vec<TransactionOffsetCommitTarget>,
    ) -> Self {
        Self::Calling { call, targets }
    }

    pub(super) fn poll(&mut self, driver: &Driver) -> TransactionOffsetCommitPoll {
        let state = mem::replace(self, Self::Consumed);
        match state {
            Self::Calling { call, targets } => {
                let Some(result) = call.try_result() else {
                    *self = Self::Calling { call, targets };
                    return TransactionOffsetCommitPoll::Pending;
                };
                drop(call);
                let mut terminal = match result {
                    Ok(outcome) => {
                        let (result, selected_version, route_token) = outcome.into_parts();
                        TransactionOffsetCommitTerminal::new(
                            selected_version,
                            result,
                            route_token,
                            targets,
                        )
                    }
                    Err(error) => return TransactionOffsetCommitPoll::Terminal(Err(error)),
                };
                let Some(token) = terminal.take_group_coordinator_refresh_token() else {
                    return TransactionOffsetCommitPoll::Terminal(Ok(terminal));
                };
                self.poll_refresh(driver, terminal, GroupCoordinatorRefresh::Queued(token))
            }
            Self::Refreshing { terminal, refresh } => self.poll_refresh(driver, terminal, refresh),
            Self::Consumed => TransactionOffsetCommitPoll::Pending,
        }
    }

    fn poll_refresh(
        &mut self,
        driver: &Driver,
        mut terminal: TransactionOffsetCommitTerminal,
        refresh: GroupCoordinatorRefresh,
    ) -> TransactionOffsetCommitPoll {
        match refresh {
            GroupCoordinatorRefresh::Queued(token) => match driver.invalidate(token) {
                Ok(call) => {
                    *self = Self::Refreshing {
                        terminal,
                        refresh: GroupCoordinatorRefresh::Active(call),
                    };
                    TransactionOffsetCommitPoll::Progress
                }
                Err(rejection) => {
                    let (_source, token) = rejection.into_parts();
                    *self = Self::Refreshing {
                        terminal,
                        refresh: GroupCoordinatorRefresh::Queued(token),
                    };
                    TransactionOffsetCommitPoll::Pending
                }
            },
            GroupCoordinatorRefresh::Active(call) => {
                if let Some(result) = call.try_result() {
                    if matches!(
                        result,
                        Ok(InvalidationDisposition::Applied
                            | InvalidationDisposition::IgnoredStale)
                    ) {
                        terminal.mark_coordinator_refresh_completed();
                    }
                    drop(call);
                    TransactionOffsetCommitPoll::Terminal(Ok(terminal))
                } else {
                    *self = Self::Refreshing {
                        terminal,
                        refresh: GroupCoordinatorRefresh::Active(call),
                    };
                    TransactionOffsetCommitPoll::Pending
                }
            }
        }
    }

    pub(super) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredTransactionOffsetCommitCall> {
        match self {
            Self::Calling { call, targets } => {
                drop(call);
                Some(RecoveredTransactionOffsetCommitCall::new(targets))
            }
            Self::Refreshing { terminal, refresh } => {
                drop(refresh);
                Some(RecoveredTransactionOffsetCommitCall::terminal(terminal))
            }
            Self::Consumed => None,
        }
    }
}

impl TransactionOffsetCommitCall {
    pub(crate) fn expire_refresh(&mut self) -> Option<TransactionOffsetCommitTerminal> {
        match mem::replace(&mut self.state, TransactionOffsetCommitCallState::Consumed) {
            TransactionOffsetCommitCallState::Refreshing { terminal, refresh } => {
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

pub(super) enum GroupCoordinatorRefresh {
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}

/// One bounded poll of an accepted call and its causal coordinator refresh.
pub(crate) enum TransactionOffsetCommitPoll {
    Pending,
    Progress,
    Terminal(Result<TransactionOffsetCommitTerminal, CompletionError>),
}
