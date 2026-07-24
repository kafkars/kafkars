//! Registry mutation for raw Fetch handoff, confirmation, restoration, and recovery.

use kafka_client_core::FetchFence;

#[cfg(test)]
use super::settlement::SettledFetchCall;

use super::{
    calls::TrackedFetchCalls,
    settlement::{
        FetchBeginSettlementError, FetchConfirmationError, FetchRestoreError, FetchRestoreFailure,
        PendingFetchConfirmation, StaleFetchConfirmationError,
    },
    stale::FetchRecovery,
    terminal::FetchTerminal,
};

impl TrackedFetchCalls {
    pub(crate) fn begin_fetch_settlement(
        &mut self,
        supplied: FetchFence,
    ) -> Result<FetchTerminal, FetchBeginSettlementError> {
        if let Some(pending) = &self.pending_confirmation {
            return Err(FetchBeginSettlementError::ConfirmationPending {
                pending: pending.fence(),
            });
        }
        let Some(settled) = self.settled.as_ref() else {
            return Err(FetchBeginSettlementError::NoSettledCall { supplied });
        };
        if settled.fence() != supplied {
            return Err(FetchBeginSettlementError::FenceMismatch {
                settled: settled.fence(),
                supplied,
            });
        }
        if settled.is_stale() {
            return Err(FetchBeginSettlementError::StaleSettledCall { supplied });
        }
        let Some(settled) = self.settled.take() else {
            return Err(FetchBeginSettlementError::NoSettledCall { supplied });
        };
        match settled.into_live_parts() {
            Ok((terminal, route_token)) => {
                self.pending_confirmation =
                    Some(PendingFetchConfirmation::new(supplied, route_token));
                Ok(terminal)
            }
            Err(settled) => {
                self.settled = Some(settled);
                Err(FetchBeginSettlementError::StaleSettledCall { supplied })
            }
        }
    }

    pub(crate) fn confirm_fetch_settlement(
        &mut self,
        supplied: FetchFence,
    ) -> Result<(), FetchConfirmationError> {
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(FetchConfirmationError::NoPendingConfirmation { supplied });
        };
        if pending.fence() != supplied {
            return Err(FetchConfirmationError::FenceMismatch {
                pending: pending.fence(),
                supplied,
            });
        }
        self.pending_confirmation = None;
        Ok(())
    }

    #[allow(
        clippy::result_large_err,
        reason = "failed restoration must return the exact raw terminal without allocation"
    )]
    pub(crate) fn restore_fetch_settlement(
        &mut self,
        terminal: FetchTerminal,
    ) -> Result<(), FetchRestoreFailure> {
        let supplied = terminal.fence();
        if self.settled.is_some() {
            return Err(FetchRestoreFailure::new(
                terminal,
                FetchRestoreError::SettledCallPresent { supplied },
            ));
        }
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(FetchRestoreFailure::new(
                terminal,
                FetchRestoreError::NoPendingConfirmation { supplied },
            ));
        };
        if pending.fence() != supplied {
            return Err(FetchRestoreFailure::new(
                terminal,
                FetchRestoreError::FenceMismatch {
                    pending: pending.fence(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(FetchRestoreFailure::new(
                terminal,
                FetchRestoreError::NoPendingConfirmation { supplied },
            ));
        };
        self.settled = Some(pending.into_settled(terminal));
        Ok(())
    }

    pub(crate) fn confirm_stale_fetch(
        &mut self,
        supplied: FetchFence,
    ) -> Result<(), StaleFetchConfirmationError> {
        let Some(settled) = self.settled.as_ref() else {
            return Err(StaleFetchConfirmationError::NoSettledCall { supplied });
        };
        if settled.fence() != supplied {
            return Err(StaleFetchConfirmationError::FenceMismatch {
                settled: settled.fence(),
                supplied,
            });
        }
        if !settled.is_stale() {
            return Err(StaleFetchConfirmationError::LiveSettledCall { supplied });
        }
        self.settled = None;
        Ok(())
    }

    pub(crate) fn recover_fetches_after_driver_shutdown(&mut self) -> FetchRecovery {
        let mut requests = self
            .calls
            .iter_mut()
            .filter_map(|call| call.request.take())
            .collect::<Vec<_>>();
        self.calls.clear();
        if let Some(settled) = self.settled.take() {
            requests.extend(settled.into_request());
        }
        self.pending_confirmation = None;
        let completion_failure = self.completion_failure.take().map(|failure| {
            let (request, observation) = failure.into_parts();
            requests.extend(request);
            observation
        });
        FetchRecovery::new(requests, completion_failure)
    }

    #[cfg(test)]
    pub(crate) fn install_terminal_for_test(&mut self, terminal: FetchTerminal) {
        self.settled = Some(SettledFetchCall::live(terminal, None));
    }
}
