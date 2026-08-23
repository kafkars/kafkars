//! Exact broker-route evidence retained beside normalized `ShareFetch` facts.

use kafka_client_core::ShareFetchBrokerId;
use kafka_driver::{RouteFailureToken, RouteKind};

#[must_use = "ShareFetch route evidence must be accepted or invalidated"]
pub(crate) struct ShareFetchRoute {
    broker_id: ShareFetchBrokerId,
    token: Option<RouteFailureToken>,
}

impl ShareFetchRoute {
    pub(super) const fn new(
        broker_id: ShareFetchBrokerId,
        token: Option<RouteFailureToken>,
    ) -> Self {
        Self { broker_id, token }
    }

    pub(crate) const fn broker_id(&self) -> ShareFetchBrokerId {
        self.broker_id
    }

    pub(crate) fn into_broker_token(self) -> Result<RouteFailureToken, Self> {
        if self.token.as_ref().map(RouteFailureToken::kind) != Some(RouteKind::Broker) {
            return Err(self);
        }
        let Self { token, .. } = self;
        Ok(token.unwrap_or_else(|| unreachable!("broker route retains its token")))
    }

    pub(crate) fn accept(self) {
        drop(self);
    }
}
