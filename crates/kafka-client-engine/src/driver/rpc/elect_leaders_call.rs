//! Linear ownership of one accepted controller election call.

use std::{error::Error, fmt};

use kafka_client_core::{ElectLeadersPlan, Moment};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::ElectLeadersResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::elect_leaders::{
        ElectLeadersDeadlineError, ElectLeadersRequestFailure, LeaderElectionRef,
        elect_leaders_request, remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner,
    elect_leaders_submission::ElectLeadersSubmitError,
    elect_leaders_terminal::{
        ElectLeadersTerminal, RecoveredElectLeadersCall, retain_elect_leaders_terminal,
    },
};

/// One accepted driver call retained beside its concrete operation owner.
#[must_use = "an accepted election call must be terminally settled"]
pub(crate) struct ElectLeadersCall {
    call: Option<RoutedCall<ElectLeadersResponse>>,
}

impl ElectLeadersCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &ElectLeadersPlan,
        request_scratch_limit: usize,
        deadline: OperationDeadline,
        now: Moment,
    ) -> Result<Self, ElectLeadersCallAdmissionFailure> {
        let timeout_ms = remaining_timeout_ms(now, deadline.core())
            .map_err(ElectLeadersCallAdmissionFailure::Deadline)?;
        let targets = change_refs(plan)?;
        let request = elect_leaders_request(
            plan.election_type(),
            &targets,
            timeout_ms,
            request_scratch_limit,
        )
        .map_err(ElectLeadersCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_elect_leaders(request, plan.election_type(), deadline.transport())
            .map_err(ElectLeadersCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    pub(crate) fn try_terminal(&mut self) -> Option<Result<ElectLeadersTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_elect_leaders_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredElectLeadersCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredElectLeadersCall::new()
        })
    }
}

fn change_refs(
    plan: &ElectLeadersPlan,
) -> Result<Vec<LeaderElectionRef<'_>>, ElectLeadersCallAdmissionFailure> {
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(plan.targets().len())
        .map_err(|_| {
            ElectLeadersCallAdmissionFailure::Request(ElectLeadersRequestFailure::RetainedBytes)
        })?;
    targets.extend(
        plan.targets()
            .iter()
            .map(|target| LeaderElectionRef::new(target.topic(), target.partition())),
    );
    Ok(targets)
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
pub(crate) enum ElectLeadersCallAdmissionFailure {
    Deadline(ElectLeadersDeadlineError),
    Request(ElectLeadersRequestFailure),
    Driver(ElectLeadersSubmitError),
}

impl fmt::Display for ElectLeadersCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadline(source) => write!(formatter, "request deadline rejected: {source:?}"),
            Self::Request(source) => write!(formatter, "request rejected: {source}"),
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for ElectLeadersCallAdmissionFailure {}
