//! Immediate transfer and exact reclamation of response-wide share deliveries.

use kafka_client_core::{GroupId, Moment};

use super::{
    fetch_delivery::ShareFetchDelivery,
    fetch_session_set::delivery::{
        ShareFetchSessionSetDeliveryError, ShareFetchSessionSetReclaimError,
    },
    port::ShareConsumerPort,
    public_registration::ShareConsumerHandle,
    registry::ShareConsumerRegistry,
    shard::ShareConsumerShardLockError,
};
use crate::consumer::{ShareConsumerBatch, ShareConsumerTryTakeBatchError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareConsumerDeliveryError {
    UnknownConsumer,
    Closing,
    MembershipFault,
    FetchFault,
    Pending,
    TransferInvariant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareConsumerDeliveryPortError {
    Closed,
    Clock,
    Lock(ShareConsumerShardLockError),
    Registry(ShareConsumerDeliveryError),
}

#[must_use = "a rejected reclaim retains the exact share delivery"]
pub(super) enum ShareConsumerDeliveryReclaimFailure {
    Unknown(ShareFetchDelivery),
    MissingSessions(ShareFetchDelivery),
    Session(ShareFetchSessionSetReclaimError),
}

impl ShareConsumerRegistry {
    pub(super) fn take_delivery(
        &mut self,
        group_id: GroupId,
        now: Moment,
    ) -> Result<Option<ShareFetchDelivery>, ShareConsumerDeliveryError> {
        let entry = self
            .entry_mut(group_id)
            .ok_or(ShareConsumerDeliveryError::UnknownConsumer)?;
        if entry.has_close() {
            return Err(ShareConsumerDeliveryError::Closing);
        }
        if entry.fault.is_some()
            || entry
                .membership
                .as_ref()
                .and_then(|membership| membership.machine().fatal())
                .is_some()
        {
            return Err(ShareConsumerDeliveryError::MembershipFault);
        }
        if entry.fetch().fault().is_some() || entry.fetch().session_fault().is_some() {
            return Err(ShareConsumerDeliveryError::FetchFault);
        }
        let Some(sessions) = entry.fetch_mut().sessions_mut() else {
            return Err(ShareConsumerDeliveryError::Pending);
        };
        if sessions.is_recovering() {
            return Err(ShareConsumerDeliveryError::Pending);
        }
        sessions.take_delivery(now).map_err(|error| match error {
            ShareFetchSessionSetDeliveryError::Cursor
            | ShareFetchSessionSetDeliveryError::Session(_) => {
                ShareConsumerDeliveryError::TransferInvariant
            }
        })
    }

    pub(super) fn reclaim_delivery(
        &mut self,
        delivery: ShareFetchDelivery,
    ) -> Result<(), ShareConsumerDeliveryReclaimFailure> {
        let group_id = delivery.group_id();
        let Some(entry) = self.entry_mut(group_id) else {
            return Err(ShareConsumerDeliveryReclaimFailure::Unknown(delivery));
        };
        let Some(sessions) = entry.fetch_mut().sessions_mut() else {
            return Err(ShareConsumerDeliveryReclaimFailure::MissingSessions(
                delivery,
            ));
        };
        sessions
            .reclaim_delivery(delivery)
            .map_err(ShareConsumerDeliveryReclaimFailure::Session)
    }
}

impl ShareConsumerPort {
    pub(in crate::consumer) fn try_take_delivery(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ShareFetchDelivery>, ShareConsumerDeliveryPortError> {
        if self.shared.admission_is_closed() {
            return Err(ShareConsumerDeliveryPortError::Closed);
        }
        let now = self
            .shared
            .clock()
            .now()
            .map_err(|_error| ShareConsumerDeliveryPortError::Clock)?;
        let mut registry = self
            .shared
            .try_registry()
            .map_err(ShareConsumerDeliveryPortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(ShareConsumerDeliveryPortError::Closed);
        }
        registry
            .take_delivery(group_id, now)
            .map_err(ShareConsumerDeliveryPortError::Registry)
    }

    pub(in crate::consumer) fn return_delivery_blocking(&self, delivery: ShareFetchDelivery) {
        self.shared.return_delivery_blocking(delivery);
    }
}

impl ShareConsumerHandle {
    /// Immediately transfers one already-authorized response-wide share delivery.
    ///
    /// This observation starts no membership, Fetch, or public timeout.
    pub fn try_take_batch(
        &mut self,
    ) -> Result<Option<ShareConsumerBatch>, ShareConsumerTryTakeBatchError> {
        self.port
            .try_take_delivery(self.group_id)
            .map(|delivery| {
                delivery.map(|delivery| {
                    ShareConsumerBatch::new(delivery, self.port.clone(), self.lifetime.clone())
                })
            })
            .map_err(ShareConsumerTryTakeBatchError::from_port)
    }
}
