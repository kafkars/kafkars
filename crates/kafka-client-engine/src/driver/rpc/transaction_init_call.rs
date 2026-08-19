//! Linear ownership and causal route refresh for one transaction identity call.

use std::{error::Error, fmt, mem, time::Instant};

use kafka_driver::{
    Call, CompletionError, Driver, InvalidationDisposition, RouteFailureToken, RoutedCall,
};
use kafka_wire::InitProducerIdResponse;

use crate::protocol::transaction::transaction_init_request;

use super::{
    super::DriverOwner,
    transaction_init_submission::TransactionInitSubmitError,
    transaction_init_terminal::{TransactionInitTerminal, retain_transaction_init_terminal},
};

#[must_use = "an accepted transaction initialization call requires terminal settlement"]
pub(crate) struct TransactionInitCall {
    driver: Driver,
    state: TransactionInitCallState,
}

impl TransactionInitCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        transactional_id: &str,
        transaction_timeout_ms: u32,
        deadline: Instant,
    ) -> Result<Self, TransactionInitCallAdmissionFailure> {
        let request = transaction_init_request(transactional_id, transaction_timeout_ms);
        let call = driver
            .submit_tracked_transaction_init(transactional_id, request, deadline)
            .map_err(TransactionInitCallAdmissionFailure::Driver)?;
        Ok(Self {
            driver: driver.driver.clone(),
            state: TransactionInitCallState::Calling(call),
        })
    }

    pub(crate) fn poll(&mut self) -> TransactionInitPoll {
        let state = mem::replace(&mut self.state, TransactionInitCallState::Consumed);
        match state {
            TransactionInitCallState::Calling(call) => {
                let Some(result) = call.try_result() else {
                    self.state = TransactionInitCallState::Calling(call);
                    return TransactionInitPoll::Pending;
                };
                drop(call);
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(error) => return TransactionInitPoll::Terminal(Err(error)),
                };
                let (result, selected_version, route_token) = outcome.into_parts();
                let mut terminal =
                    retain_transaction_init_terminal(selected_version, result, route_token);
                let Some(token) = terminal.take_transaction_coordinator_refresh_token() else {
                    return TransactionInitPoll::Terminal(Ok(terminal));
                };
                self.poll_refresh(terminal, TransactionInitCoordinatorRefresh::Queued(token))
            }
            TransactionInitCallState::Refreshing { terminal, refresh } => {
                self.poll_refresh(terminal, refresh)
            }
            TransactionInitCallState::Consumed => TransactionInitPoll::Pending,
        }
    }

    /// Stops only a post-terminal coordinator refresh at the public deadline.
    pub(crate) fn expire_refresh(&mut self) -> Option<TransactionInitTerminal> {
        match mem::replace(&mut self.state, TransactionInitCallState::Consumed) {
            TransactionInitCallState::Refreshing { terminal, refresh } => {
                drop(refresh);
                Some(terminal)
            }
            state => {
                self.state = state;
                None
            }
        }
    }

    fn poll_refresh(
        &mut self,
        mut terminal: TransactionInitTerminal,
        refresh: TransactionInitCoordinatorRefresh,
    ) -> TransactionInitPoll {
        match refresh {
            TransactionInitCoordinatorRefresh::Queued(token) => {
                match self.driver.invalidate(token) {
                    Ok(call) => {
                        self.state = TransactionInitCallState::Refreshing {
                            terminal,
                            refresh: TransactionInitCoordinatorRefresh::Active(call),
                        };
                        TransactionInitPoll::Progress
                    }
                    Err(rejection) => {
                        let (_source, token) = rejection.into_parts();
                        self.state = TransactionInitCallState::Refreshing {
                            terminal,
                            refresh: TransactionInitCoordinatorRefresh::Queued(token),
                        };
                        TransactionInitPoll::Pending
                    }
                }
            }
            TransactionInitCoordinatorRefresh::Active(call) => {
                if let Some(result) = call.try_result() {
                    if matches!(
                        result,
                        Ok(InvalidationDisposition::Applied
                            | InvalidationDisposition::IgnoredStale)
                    ) {
                        terminal.mark_coordinator_refresh_completed();
                    }
                    drop(call);
                    TransactionInitPoll::Terminal(Ok(terminal))
                } else {
                    self.state = TransactionInitCallState::Refreshing {
                        terminal,
                        refresh: TransactionInitCoordinatorRefresh::Active(call),
                    };
                    TransactionInitPoll::Pending
                }
            }
            #[cfg(test)]
            TransactionInitCoordinatorRefresh::ScriptedForTest { progress_reported } => {
                self.state = TransactionInitCallState::Refreshing {
                    terminal,
                    refresh: TransactionInitCoordinatorRefresh::ScriptedForTest {
                        progress_reported: true,
                    },
                };
                if progress_reported {
                    TransactionInitPoll::Pending
                } else {
                    TransactionInitPoll::Progress
                }
            }
        }
    }

    pub(crate) fn recover_after_driver_shutdown(self) -> Option<TransactionInitTerminal> {
        match self.state {
            TransactionInitCallState::Calling(call) => {
                drop(call);
                None
            }
            TransactionInitCallState::Refreshing { terminal, refresh } => {
                drop(refresh);
                Some(terminal)
            }
            TransactionInitCallState::Consumed => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn refreshing_for_test(driver: &DriverOwner, error_code: i16) -> Self {
        let mut response = InitProducerIdResponse::default();
        response.error_code = error_code;
        Self {
            driver: driver.driver.clone(),
            state: TransactionInitCallState::Refreshing {
                terminal: retain_transaction_init_terminal(
                    Some(kafka_driver::ApiVersion::new(5)),
                    Ok(response),
                    None,
                ),
                refresh: TransactionInitCoordinatorRefresh::ScriptedForTest {
                    progress_reported: false,
                },
            },
        }
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "the linear state retains the exact initialization terminal until refresh settles"
)]
enum TransactionInitCallState {
    Calling(RoutedCall<InitProducerIdResponse>),
    Refreshing {
        terminal: TransactionInitTerminal,
        refresh: TransactionInitCoordinatorRefresh,
    },
    Consumed,
}

enum TransactionInitCoordinatorRefresh {
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
    #[cfg(test)]
    ScriptedForTest {
        progress_reported: bool,
    },
}

/// One bounded observation of the request and any causal coordinator refresh.
pub(crate) enum TransactionInitPoll {
    Pending,
    Progress,
    Terminal(Result<TransactionInitTerminal, CompletionError>),
}

#[must_use = "a rejected call must become a definitely-unsent core fact"]
#[derive(Debug)]
pub(crate) enum TransactionInitCallAdmissionFailure {
    Driver(TransactionInitSubmitError),
}

impl fmt::Display for TransactionInitCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for TransactionInitCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}
