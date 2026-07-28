//! Exact coordinator target, protocol intent, and retained bounds across call states.

use kafka_client_core::AdminDescribeConsumerGroupsCallKind;

#[derive(Debug, Eq, PartialEq)]
pub(in crate::driver::rpc) struct DescribeConsumerGroupsEvidence {
    group_id: String,
    include_authorized_operations: bool,
    call_kind: AdminDescribeConsumerGroupsCallKind,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl DescribeConsumerGroupsEvidence {
    pub(in crate::driver::rpc) const fn new(
        group_id: String,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            group_id,
            include_authorized_operations,
            call_kind,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) fn group_id(&self) -> &str {
        &self.group_id
    }

    pub(in crate::driver::rpc) const fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    pub(in crate::driver::rpc) const fn call_kind(&self) -> AdminDescribeConsumerGroupsCallKind {
        self.call_kind
    }

    pub(in crate::driver::rpc) const fn request_scratch_limit(&self) -> usize {
        self.request_scratch_limit
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        group_id: &str,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.group_id == group_id
            && self.include_authorized_operations == include_authorized_operations
            && self.call_kind == call_kind
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(
        self,
    ) -> (
        String,
        bool,
        AdminDescribeConsumerGroupsCallKind,
        usize,
        usize,
    ) {
        (
            self.group_id,
            self.include_authorized_operations,
            self.call_kind,
            self.request_scratch_limit,
            self.result_limit,
        )
    }
}
