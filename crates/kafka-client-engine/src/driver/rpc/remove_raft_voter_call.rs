//! Linear ownership of one accepted tracked AnyBroker voter removal.

use std::{error::Error, fmt};

use kafka_client_core::RemoveRaftVoterPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::RemoveRaftVoterResponse;

use crate::{
    clock::OperationDeadline, protocol::admin::remove_raft_voter::remove_raft_voter_request,
};

use super::{
    super::DriverOwner,
    remove_raft_voter_submission::RemoveRaftVoterSubmitError,
    remove_raft_voter_terminal::{
        RecoveredRemoveRaftVoterCall, RemoveRaftVoterRawTerminal, retain_remove_raft_voter_terminal,
    },
};

/// One accepted API81 call retained beside its deterministic owner.
#[must_use = "an accepted RemoveRaftVoter call must be terminally settled"]
pub(crate) struct RemoveRaftVoterCall {
    call: Option<RoutedCall<RemoveRaftVoterResponse>>,
}

impl RemoveRaftVoterCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &RemoveRaftVoterPlan,
        deadline: OperationDeadline,
    ) -> Result<Self, RemoveRaftVoterCallAdmissionFailure> {
        let request = remove_raft_voter_request(plan);
        let call = driver
            .submit_tracked_remove_raft_voter(request, deadline.transport())
            .map_err(RemoveRaftVoterCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing its route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<RemoveRaftVoterRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_remove_raft_voter_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(mut self) -> Option<RecoveredRemoveRaftVoterCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredRemoveRaftVoterCall
        })
    }
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
pub(crate) enum RemoveRaftVoterCallAdmissionFailure {
    Driver(RemoveRaftVoterSubmitError),
}

impl fmt::Display for RemoveRaftVoterCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for RemoveRaftVoterCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}
