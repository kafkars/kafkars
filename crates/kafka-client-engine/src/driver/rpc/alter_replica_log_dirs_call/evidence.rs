//! Exact broker group and retained bounds across destructive call states.

use kafka_client_core::AlterReplicaLogDirAssignment;

#[derive(Debug)]
pub(in crate::driver::rpc) struct AlterReplicaLogDirsEvidence {
    broker_id: i32,
    assignments: Vec<AlterReplicaLogDirAssignment>,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl AlterReplicaLogDirsEvidence {
    pub(in crate::driver::rpc) const fn new(
        broker_id: i32,
        assignments: Vec<AlterReplicaLogDirAssignment>,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            broker_id,
            assignments,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    pub(in crate::driver::rpc) fn assignments(&self) -> &[AlterReplicaLogDirAssignment] {
        &self.assignments
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        broker_id: i32,
        assignments: &[AlterReplicaLogDirAssignment],
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.broker_id == broker_id
            && self.assignments == assignments
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(
        self,
    ) -> (i32, Vec<AlterReplicaLogDirAssignment>, usize, usize) {
        (
            self.broker_id,
            self.assignments,
            self.request_scratch_limit,
            self.result_limit,
        )
    }
}
