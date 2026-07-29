//! Exact discovery or broker-attempt evidence retained across linear call states.

use kafka_client_core::AdminListTransactionsPlan;

pub(crate) enum ListTransactionsCorrelation {
    Discovery {
        retained_limit: usize,
    },
    Broker {
        broker_id: i32,
        plan: AdminListTransactionsPlan,
        retained_limit: usize,
    },
}

impl ListTransactionsCorrelation {
    pub(crate) const fn discovery(retained_limit: usize) -> Self {
        Self::Discovery { retained_limit }
    }

    pub(crate) const fn broker(
        broker_id: i32,
        plan: AdminListTransactionsPlan,
        retained_limit: usize,
    ) -> Self {
        Self::Broker {
            broker_id,
            plan,
            retained_limit,
        }
    }

    pub(crate) fn into_submission_evidence(
        self,
    ) -> (Option<(i32, AdminListTransactionsPlan)>, usize) {
        match self {
            Self::Discovery { retained_limit } => (None, retained_limit),
            Self::Broker {
                broker_id,
                plan,
                retained_limit,
            } => (Some((broker_id, plan)), retained_limit),
        }
    }

    pub(crate) const fn retained_limit(&self) -> usize {
        match self {
            Self::Discovery { retained_limit } | Self::Broker { retained_limit, .. } => {
                *retained_limit
            }
        }
    }

    pub(crate) const fn broker_id(&self) -> Option<i32> {
        match self {
            Self::Discovery { .. } => None,
            Self::Broker { broker_id, .. } => Some(*broker_id),
        }
    }

    pub(crate) fn matches_discovery(&self, retained_limit: usize) -> bool {
        matches!(
            self,
            Self::Discovery {
                retained_limit: actual,
            } if *actual == retained_limit
        )
    }

    pub(crate) fn matches_broker(
        &self,
        broker_id: i32,
        plan: &AdminListTransactionsPlan,
        retained_limit: usize,
    ) -> bool {
        matches!(
            self,
            Self::Broker {
                broker_id: actual_broker,
                plan: actual_plan,
                retained_limit: actual_limit,
            } if *actual_broker == broker_id
                && actual_plan == plan
                && *actual_limit == retained_limit
        )
    }
}
