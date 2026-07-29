//! Test-only observations of retained Admin `DescribeProducers` ownership.

use super::super::{AdminDescribeProducersHost, AdminDescribeProducersHostError};

impl AdminDescribeProducersHost {
    pub(in crate::admin::describe_producers) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeProducersCall::for_test());
    }

    pub(in crate::admin::describe_producers) fn recovered_call_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::describe_producers) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AdminDescribeProducersHostError> {
        self.settle_recovered_transport(0)
    }
}
