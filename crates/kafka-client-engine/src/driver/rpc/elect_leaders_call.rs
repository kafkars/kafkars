//! Linear ownership of one accepted controller election call.

mod correlation;

use std::{error::Error, fmt};

use kafka_client_core::{ElectLeadersPlan, LeaderElectionTarget, Moment};
use kafka_driver::{CompletionError, Driver, RoutedCall};
use kafka_wire::ElectLeadersResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::elect_leaders::{
        ElectLeadersDeadlineError, ElectLeadersRequestFailure, ElectLeadersSelectionRef,
        LeaderElectionRef, elect_leaders_request, remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner,
    elect_leaders_submission::ElectLeadersSubmitError,
    elect_leaders_terminal::{
        ElectLeadersTerminal, RecoveredElectLeadersCall, retain_elect_leaders_terminal,
    },
};

pub(super) use correlation::ElectLeadersCorrelation;

/// One accepted driver call retained beside its concrete operation owner.
#[must_use = "an accepted election call must be terminally settled"]
pub(crate) struct ElectLeadersCall {
    driver: Option<Driver>,
    call: Option<RoutedCall<ElectLeadersResponse>>,
    correlation: Option<ElectLeadersCorrelation>,
}

impl ElectLeadersCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: ElectLeadersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
        deadline: OperationDeadline,
        now: Moment,
    ) -> Result<Self, ElectLeadersCallAdmissionFailure> {
        let correlation = ElectLeadersCorrelation::new(plan, request_scratch_limit, result_limit);
        let timeout_ms = match remaining_timeout_ms(now, deadline.core()) {
            Ok(timeout_ms) => timeout_ms,
            Err(source) => {
                return Err(ElectLeadersCallAdmissionFailure::new(
                    ElectLeadersCallAdmissionFailureSource::Deadline(source),
                    correlation,
                ));
            }
        };
        let request = match correlation.plan().selection().selected_targets() {
            None => elect_leaders_request(
                correlation.plan().election_type(),
                ElectLeadersSelectionRef::AllPartitions,
                timeout_ms,
                request_scratch_limit,
            ),
            Some(selected) => {
                let targets = match change_refs(selected) {
                    Ok(targets) => targets,
                    Err(source) => {
                        return Err(ElectLeadersCallAdmissionFailure::new(
                            ElectLeadersCallAdmissionFailureSource::Request(source),
                            correlation,
                        ));
                    }
                };
                elect_leaders_request(
                    correlation.plan().election_type(),
                    ElectLeadersSelectionRef::Selected(&targets),
                    timeout_ms,
                    request_scratch_limit,
                )
            }
        };
        let request = match request {
            Ok(request) => request,
            Err(source) => {
                return Err(ElectLeadersCallAdmissionFailure::new(
                    ElectLeadersCallAdmissionFailureSource::Request(source),
                    correlation,
                ));
            }
        };
        let call = match driver.submit_tracked_elect_leaders(
            request,
            correlation.plan().election_type(),
            deadline.transport(),
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(ElectLeadersCallAdmissionFailure::new(
                    ElectLeadersCallAdmissionFailureSource::Driver(source),
                    correlation,
                ));
            }
        };
        Ok(Self {
            driver: Some(driver.driver.clone()),
            call: Some(call),
            correlation: Some(correlation),
        })
    }

    pub(crate) fn try_terminal(&mut self) -> Option<Result<ElectLeadersTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let correlation = self.correlation.take()?;
                let driver = self.driver.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_elect_leaders_terminal(
                    driver,
                    selected_version,
                    result,
                    route_token,
                    correlation,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches_correlation(
        &self,
        plan: &ElectLeadersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.correlation.as_ref().is_some_and(|correlation| {
            correlation.matches(plan, request_scratch_limit, result_limit)
        })
    }

    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredElectLeadersCall> {
        let Self {
            driver,
            call,
            correlation,
        } = self;
        drop(driver);
        match (call, correlation) {
            (Some(call), Some(correlation)) => {
                drop(call);
                Some(RecoveredElectLeadersCall::new(correlation))
            }
            _ => None,
        }
    }
}

fn change_refs(
    selected: &[LeaderElectionTarget],
) -> Result<Vec<LeaderElectionRef<'_>>, ElectLeadersRequestFailure> {
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(selected.len())
        .map_err(|_| ElectLeadersRequestFailure::RetainedBytes)?;
    targets.extend(
        selected
            .iter()
            .map(|target| LeaderElectionRef::new(target.topic(), target.partition())),
    );
    Ok(targets)
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
enum ElectLeadersCallAdmissionFailureSource {
    Deadline(ElectLeadersDeadlineError),
    Request(ElectLeadersRequestFailure),
    Driver(ElectLeadersSubmitError),
}

/// Definitely-unsent rejection retaining the exact attempted election.
#[derive(Debug)]
#[must_use = "a rejected election call must become deterministic input"]
pub(crate) struct ElectLeadersCallAdmissionFailure {
    source: ElectLeadersCallAdmissionFailureSource,
    correlation: ElectLeadersCorrelation,
}

impl ElectLeadersCallAdmissionFailure {
    const fn new(
        source: ElectLeadersCallAdmissionFailureSource,
        correlation: ElectLeadersCorrelation,
    ) -> Self {
        Self {
            source,
            correlation,
        }
    }

    pub(crate) fn into_correlation(self) -> (ElectLeadersPlan, usize, usize) {
        self.correlation.into_parts()
    }
}

impl fmt::Display for ElectLeadersCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            ElectLeadersCallAdmissionFailureSource::Deadline(source) => {
                write!(formatter, "request deadline rejected: {source:?}")
            }
            ElectLeadersCallAdmissionFailureSource::Request(source) => {
                write!(formatter, "request rejected: {source}")
            }
            ElectLeadersCallAdmissionFailureSource::Driver(source) => {
                write!(formatter, "{source}")
            }
        }
    }
}

impl Error for ElectLeadersCallAdmissionFailure {}
