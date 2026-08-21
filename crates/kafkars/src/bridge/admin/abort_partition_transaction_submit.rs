//! Admission handoff for public partition-transaction abort work.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::abort_partition_transaction::{
    AbortPartitionTransactionAdminRequest, AdminAbortPartitionTransaction,
};

impl AdminEngine {
    pub(crate) fn submit_abort_partition_transaction(
        &self,
        request: AbortPartitionTransactionAdminRequest,
        timeout: Duration,
    ) -> AdminAbortPartitionTransaction {
        AdminAbortPartitionTransaction::from_admission(
            self.handle
                .try_abort_partition_transaction(request.into_engine(), timeout),
        )
    }
}
