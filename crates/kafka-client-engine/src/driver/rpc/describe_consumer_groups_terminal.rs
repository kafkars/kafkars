//! Neutral borrowed terminal facts with opaque coordinator-route ownership.

mod recovered;

use kafka_client_core::{AdminDescribeConsumerGroupsCallKind, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeGroupsResponse;

use super::{
    super::request_failure_delivery,
    consumer_group_describe_terminal::{
        ConsumerGroupDescribeRawTerminal, ConsumerGroupDescribeTerminalFact,
    },
    describe_consumer_groups_call::DescribeConsumerGroupsEvidence,
};

pub(crate) use recovered::RecoveredDescribeConsumerGroupsCall;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeConsumerGroupsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for a concrete host interpreter.
pub(crate) enum DescribeConsumerGroupsTerminalFact<'a> {
    Consumer(ConsumerGroupDescribeTerminalFact<'a>),
    ClassicResponse {
        selected_version: Option<i16>,
        response: &'a DescribeGroupsResponse,
    },
    ClassicFailed {
        kind: DescribeConsumerGroupsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained through protocol validation and core settlement.
#[must_use = "a raw DescribeConsumerGroups terminal owns unsettled route evidence"]
pub(crate) struct DescribeConsumerGroupsTerminal {
    inner: DescribeConsumerGroupsTerminalInner,
    evidence: DescribeConsumerGroupsEvidence,
}

enum DescribeConsumerGroupsTerminalInner {
    Consumer(ConsumerGroupDescribeRawTerminal),
    Classic {
        selected_version: Option<i16>,
        result: Result<DescribeGroupsResponse, RequestError>,
        route_token: Option<RouteFailureToken>,
    },
}

impl DescribeConsumerGroupsTerminal {
    #[cfg(test)]
    pub(crate) fn for_test(
        group_id: String,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            inner: DescribeConsumerGroupsTerminalInner::Classic {
                selected_version: Some(6),
                result: Ok(DescribeGroupsResponse::default()),
                route_token: None,
            },
            evidence: DescribeConsumerGroupsEvidence::new(
                group_id,
                include_authorized_operations,
                call_kind,
                request_scratch_limit,
                result_limit,
            ),
        }
    }

    pub(crate) fn fact(&self) -> DescribeConsumerGroupsTerminalFact<'_> {
        match &self.inner {
            DescribeConsumerGroupsTerminalInner::Consumer(terminal) => {
                DescribeConsumerGroupsTerminalFact::Consumer(terminal.fact())
            }
            DescribeConsumerGroupsTerminalInner::Classic {
                selected_version,
                result: Ok(response),
                ..
            } => DescribeConsumerGroupsTerminalFact::ClassicResponse {
                selected_version: *selected_version,
                response,
            },
            DescribeConsumerGroupsTerminalInner::Classic {
                result: Err(error), ..
            } => DescribeConsumerGroupsTerminalFact::ClassicFailed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) fn group_id(&self) -> &str {
        self.evidence.group_id()
    }

    pub(crate) const fn include_authorized_operations(&self) -> bool {
        self.evidence.include_authorized_operations()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    pub(crate) fn matches_evidence(
        &self,
        group_id: &str,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence.matches(
            group_id,
            include_authorized_operations,
            call_kind,
            request_scratch_limit,
            result_limit,
        )
    }

    /// Deliberately releases route evidence after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self { inner, evidence } = self;
        match inner {
            DescribeConsumerGroupsTerminalInner::Consumer(terminal) => terminal.discard(),
            DescribeConsumerGroupsTerminalInner::Classic { route_token, .. } => drop(route_token),
        }
        drop(evidence);
    }

    pub(super) const fn from_consumer(
        terminal: ConsumerGroupDescribeRawTerminal,
        evidence: DescribeConsumerGroupsEvidence,
    ) -> Self {
        Self {
            inner: DescribeConsumerGroupsTerminalInner::Consumer(terminal),
            evidence,
        }
    }
}

pub(super) fn retain_describe_consumer_groups_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeGroupsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: DescribeConsumerGroupsEvidence,
) -> DescribeConsumerGroupsTerminal {
    DescribeConsumerGroupsTerminal {
        inner: DescribeConsumerGroupsTerminalInner::Classic {
            selected_version: selected_version.map(ApiVersion::value),
            result,
            route_token,
        },
        evidence,
    }
}

fn failure_kind(error: &RequestError) -> DescribeConsumerGroupsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeConsumerGroupsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeConsumerGroupsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeConsumerGroupsDriverFailureKind::Compatibility
        }
        _ => DescribeConsumerGroupsDriverFailureKind::Transport,
    }
}
