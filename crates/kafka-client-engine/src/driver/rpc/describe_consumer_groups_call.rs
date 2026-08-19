//! Linear ownership of one modern or classic group-description call.

mod evidence;

use kafka_client_core::AdminDescribeConsumerGroupsCallKind;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeGroupsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::describe_consumer_groups::describe_consumer_group_request,
};

use super::{
    super::DriverOwner,
    consumer_group_describe_call::{
        ConsumerGroupDescribeCall, ConsumerGroupDescribeCallAdmissionFailure,
    },
    describe_consumer_groups_submission::DescribeConsumerGroupsSubmitError,
    describe_consumer_groups_terminal::{
        DescribeConsumerGroupsTerminal, RecoveredDescribeConsumerGroupsCall,
        retain_describe_consumer_groups_terminal,
    },
};

pub(super) use evidence::DescribeConsumerGroupsEvidence;

/// One accepted driver call retained beside its concrete operation owner.
#[must_use = "an accepted DescribeConsumerGroups call must be terminally settled"]
pub(crate) struct DescribeConsumerGroupsCall {
    inner: DescribeConsumerGroupsCallInner,
    evidence: Option<DescribeConsumerGroupsEvidence>,
}

enum DescribeConsumerGroupsCallInner {
    Consumer(ConsumerGroupDescribeCall),
    Classic(Option<RoutedCall<DescribeGroupsResponse>>),
}

impl DescribeConsumerGroupsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        group_id: String,
        include_authorized_operations: bool,
        request_scratch_limit: usize,
        result_limit: usize,
        deadline: OperationDeadline,
    ) -> Result<Self, DescribeConsumerGroupsCallAdmissionFailure> {
        let evidence = DescribeConsumerGroupsEvidence::new(
            group_id,
            include_authorized_operations,
            call_kind,
            request_scratch_limit,
            result_limit,
        );
        let inner = match evidence.call_kind() {
            AdminDescribeConsumerGroupsCallKind::Consumer => {
                let call = match ConsumerGroupDescribeCall::submit(
                    driver,
                    evidence.group_id(),
                    evidence.include_authorized_operations(),
                    evidence.request_scratch_limit(),
                    deadline,
                ) {
                    Ok(call) => call,
                    Err(source) => {
                        return Err(DescribeConsumerGroupsCallAdmissionFailure::new(
                            DescribeConsumerGroupsCallAdmissionSource::Consumer(source),
                            evidence,
                        ));
                    }
                };
                DescribeConsumerGroupsCallInner::Consumer(call)
            }
            AdminDescribeConsumerGroupsCallKind::Classic
            | AdminDescribeConsumerGroupsCallKind::ClassicFallback => {
                let request = describe_consumer_group_request(
                    evidence.group_id(),
                    evidence.include_authorized_operations(),
                );
                let call = match driver.submit_tracked_describe_consumer_group(
                    evidence.group_id(),
                    request,
                    deadline.transport(),
                    evidence.include_authorized_operations(),
                ) {
                    Ok(call) => call,
                    Err(source) => {
                        return Err(DescribeConsumerGroupsCallAdmissionFailure::new(
                            DescribeConsumerGroupsCallAdmissionSource::Classic(source),
                            evidence,
                        ));
                    }
                };
                DescribeConsumerGroupsCallInner::Classic(Some(call))
            }
        };
        Ok(Self {
            inner,
            evidence: Some(evidence),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeConsumerGroupsTerminal, CompletionError>> {
        match &mut self.inner {
            DescribeConsumerGroupsCallInner::Consumer(call) => {
                let result = call.try_terminal()?;
                Some(match result {
                    Ok(terminal) => {
                        let evidence = self.evidence.take()?;
                        Ok(DescribeConsumerGroupsTerminal::from_consumer(
                            terminal, evidence,
                        ))
                    }
                    Err(source) => Err(source),
                })
            }
            DescribeConsumerGroupsCallInner::Classic(call) => {
                let result = call.as_mut()?.try_result()?;
                match result {
                    Ok(outcome) => {
                        let evidence = self.evidence.take()?;
                        drop(call.take());
                        let (result, selected_version, route_token) = outcome.into_parts();
                        Some(Ok(retain_describe_consumer_groups_terminal(
                            selected_version,
                            result,
                            route_token,
                            evidence,
                        )))
                    }
                    Err(source) => Some(Err(source)),
                }
            }
        }
    }

    pub(crate) fn matches_evidence(
        &self,
        group_id: &str,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence.as_ref().is_some_and(|evidence| {
            evidence.matches(
                group_id,
                include_authorized_operations,
                call_kind,
                request_scratch_limit,
                result_limit,
            )
        })
    }

    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredDescribeConsumerGroupsCall> {
        let Self { inner, evidence } = self;
        match inner {
            DescribeConsumerGroupsCallInner::Consumer(call) => call
                .recover_after_driver_shutdown()
                .zip(evidence)
                .map(|(recovered, evidence)| {
                    RecoveredDescribeConsumerGroupsCall::from_consumer(recovered, evidence)
                }),
            DescribeConsumerGroupsCallInner::Classic(call) => {
                call.zip(evidence).map(|(call, evidence)| {
                    drop(call);
                    RecoveredDescribeConsumerGroupsCall::new(evidence)
                })
            }
        }
    }
}
#[derive(Debug)]
enum DescribeConsumerGroupsCallAdmissionSource {
    Consumer(ConsumerGroupDescribeCallAdmissionFailure),
    Classic(DescribeConsumerGroupsSubmitError),
}

impl DescribeConsumerGroupsCallAdmissionSource {
    fn discard(self) {
        match self {
            Self::Consumer(source) => drop(source),
            Self::Classic(source) => drop(source),
        }
    }
}

/// Definitely-unsent coordinator validation or bounded-driver rejection.
#[must_use = "a rejected DescribeConsumerGroups call must become operation input"]
pub(crate) struct DescribeConsumerGroupsCallAdmissionFailure {
    source: DescribeConsumerGroupsCallAdmissionSource,
    evidence: DescribeConsumerGroupsEvidence,
}

impl DescribeConsumerGroupsCallAdmissionFailure {
    const fn new(
        source: DescribeConsumerGroupsCallAdmissionSource,
        evidence: DescribeConsumerGroupsEvidence,
    ) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_evidence(
        self,
    ) -> (
        String,
        bool,
        AdminDescribeConsumerGroupsCallKind,
        usize,
        usize,
    ) {
        let Self { source, evidence } = self;
        source.discard();
        evidence.into_parts()
    }
}
