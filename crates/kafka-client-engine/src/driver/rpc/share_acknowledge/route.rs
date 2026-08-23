//! Exact broker-route evidence retained beside normalized acknowledgement facts.

use kafka_client_core::ShareFetchBrokerId;
use kafka_driver::RouteFailureToken;

#[must_use = "ShareAcknowledge route evidence must be accepted or invalidated"]
pub(crate) struct ShareAcknowledgeRoute {
    broker_id: ShareFetchBrokerId,
    token: Option<RouteFailureToken>,
}

impl ShareAcknowledgeRoute {
    pub(super) const fn new(
        broker_id: ShareFetchBrokerId,
        token: Option<RouteFailureToken>,
    ) -> Self {
        Self { broker_id, token }
    }

    pub(crate) const fn broker_id(&self) -> ShareFetchBrokerId {
        self.broker_id
    }

    #[cfg(test)]
    pub(crate) const fn without_token_for_test(broker_id: ShareFetchBrokerId) -> Self {
        Self::new(broker_id, None)
    }

    pub(crate) fn accept(self) {
        let Self {
            broker_id: _broker_id,
            token,
        } = self;
        drop(token);
    }
}
