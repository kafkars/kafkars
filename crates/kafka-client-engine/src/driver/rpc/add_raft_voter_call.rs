//! Linear ownership of one accepted tracked controller-routed voter addition.

use std::{error::Error, fmt};

use kafka_client_core::{AddRaftVoterPlan, Moment};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::AddRaftVoterResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::add_raft_voter::{
        AddRaftVoterDeadlineError, AddRaftVoterRequestFailure, add_raft_voter_request,
        remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner,
    add_raft_voter_submission::AddRaftVoterSubmitError,
    add_raft_voter_terminal::{
        AddRaftVoterRawTerminal, RecoveredAddRaftVoterCall, retain_add_raft_voter_terminal,
    },
};

/// One accepted API80 call retained beside its deterministic owner.
#[must_use = "an accepted AddRaftVoter call must be terminally settled"]
pub(crate) struct AddRaftVoterCall {
    call: Option<RoutedCall<AddRaftVoterResponse>>,
    plan: Option<AddRaftVoterPlan>,
}

impl AddRaftVoterCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &AddRaftVoterPlan,
        deadline: OperationDeadline,
        now: Moment,
    ) -> Result<Self, AddRaftVoterCallAdmissionFailure> {
        let timeout_ms = remaining_timeout_ms(now, deadline.core())
            .map_err(AddRaftVoterCallAdmissionFailure::Deadline)?;
        let request = add_raft_voter_request(plan, timeout_ms)
            .map_err(AddRaftVoterCallAdmissionFailure::Request)?;
        let correlation_plan = plan.clone();
        let call = driver
            .submit_tracked_add_raft_voter(plan, request, deadline.transport())
            .map_err(AddRaftVoterCallAdmissionFailure::Driver)?;
        Ok(Self {
            call: Some(call),
            plan: Some(correlation_plan),
        })
    }

    /// Extracts one ready raw terminal without releasing its route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AddRaftVoterRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let plan = self.plan.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_add_raft_voter_terminal(
                    selected_version,
                    result,
                    route_token,
                    plan,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredAddRaftVoterCall> {
        let Self { call, plan } = self;
        match (call, plan) {
            (Some(call), Some(plan)) => {
                drop(call);
                Some(RecoveredAddRaftVoterCall::new(plan))
            }
            _ => None,
        }
    }
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
pub(crate) enum AddRaftVoterCallAdmissionFailure {
    Deadline(AddRaftVoterDeadlineError),
    Request(AddRaftVoterRequestFailure),
    Driver(AddRaftVoterSubmitError),
}

impl fmt::Display for AddRaftVoterCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadline(source) => write!(formatter, "request deadline rejected: {source:?}"),
            Self::Request(source) => write!(formatter, "request rejected: {source:?}"),
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for AddRaftVoterCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
            Self::Deadline(_) | Self::Request(_) => None,
        }
    }
}
