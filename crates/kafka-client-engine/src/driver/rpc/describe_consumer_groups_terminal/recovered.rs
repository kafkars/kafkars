//! Exact group-description evidence recovered after unique-driver shutdown.

use kafka_client_core::AdminDescribeConsumerGroupsCallKind;

use super::super::{
    consumer_group_describe_terminal::RecoveredConsumerGroupDescribeCall,
    describe_consumer_groups_call::DescribeConsumerGroupsEvidence,
};

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered DescribeConsumerGroups ownership still requires settlement"]
pub(crate) struct RecoveredDescribeConsumerGroupsCall {
    inner: RecoveredDescribeConsumerGroupsCallInner,
    evidence: DescribeConsumerGroupsEvidence,
}

enum RecoveredDescribeConsumerGroupsCallInner {
    Consumer(RecoveredConsumerGroupDescribeCall),
    Classic,
}

impl RecoveredDescribeConsumerGroupsCall {
    pub(in crate::driver::rpc) const fn new(evidence: DescribeConsumerGroupsEvidence) -> Self {
        Self {
            inner: RecoveredDescribeConsumerGroupsCallInner::Classic,
            evidence,
        }
    }

    pub(in crate::driver::rpc) const fn from_consumer(
        recovered: RecoveredConsumerGroupDescribeCall,
        evidence: DescribeConsumerGroupsEvidence,
    ) -> Self {
        Self {
            inner: RecoveredDescribeConsumerGroupsCallInner::Consumer(recovered),
            evidence,
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
        self.evidence.matches(
            group_id,
            include_authorized_operations,
            call_kind,
            request_scratch_limit,
            result_limit,
        )
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        let Self { inner, evidence } = self;
        match inner {
            RecoveredDescribeConsumerGroupsCallInner::Consumer(recovered) => recovered.seal(),
            RecoveredDescribeConsumerGroupsCallInner::Classic => {}
        }
        drop(evidence);
    }
}
