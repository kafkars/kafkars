//! Test-only observations of retained Admin `DescribeTransactions` ownership.

use super::super::{AdminDescribeTransactionsHost, AdminDescribeTransactionsHostError};

impl AdminDescribeTransactionsHost {
    pub(in crate::admin::describe_transactions) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeTransactionsCall::for_test());
    }

    pub(in crate::admin::describe_transactions) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::describe_transactions) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AdminDescribeTransactionsHostError> {
        self.settle_recovered_transport(0)
    }
}
