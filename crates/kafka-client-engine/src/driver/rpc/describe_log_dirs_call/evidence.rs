//! Exact broker, ordered selection, and capacity evidence for one routed call.

use kafka_client_core::AdminDescribeLogDirsSelection;

pub(in crate::driver::rpc) struct DescribeLogDirsEvidence {
    broker_id: i32,
    selection: AdminDescribeLogDirsSelection,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl DescribeLogDirsEvidence {
    pub(in crate::driver::rpc) const fn new(
        broker_id: i32,
        selection: AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            broker_id,
            selection,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) const fn selection(&self) -> &AdminDescribeLogDirsSelection {
        &self.selection
    }

    pub(in crate::driver::rpc) const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        broker_id: i32,
        selection: &AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.broker_id == broker_id
            && self.selection == *selection
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(
        self,
    ) -> (i32, AdminDescribeLogDirsSelection, usize, usize) {
        (
            self.broker_id,
            self.selection,
            self.request_scratch_limit,
            self.result_limit,
        )
    }
}
