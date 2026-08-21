//! Isolated `ListTransactions` submission from facade values into the engine owner.

use std::time::Duration;

use crate::bridge::admin_list_transactions::{AdminListTransactions, ListTransactionsAdminRequest};

use super::AdminEngine;

impl AdminEngine {
    pub(crate) fn submit_list_transactions(
        &self,
        request: ListTransactionsAdminRequest,
        timeout: Duration,
    ) -> AdminListTransactions {
        AdminListTransactions::from_admission(
            self.handle
                .try_list_transactions(request.into_engine(), timeout),
        )
    }
}
