//! Linear engine ownership of one complete decoded `ShareFetch` response.

use std::sync::Arc;

use kafka_client_core::{
    AssignedTopicPartition, GroupId, Moment, ShareAcquisition, ShareAcquisitionAdmissionErrorKind,
    ShareFetchSessionFence, ShareTopicUuid,
};

use crate::protocol::fetch::FetchBatch;

use super::{
    fetch_session::ShareFetchSessionOwner, fetch_session_settlement::StagedShareFetchDelivery,
};

/// One named partition and its decoded record batches inside a share delivery.
#[must_use = "decoded share records must remain with their exact acquisition batch"]
pub(in crate::consumer) struct ShareFetchDeliveryPartition {
    topic: Arc<str>,
    topic_uuid: ShareTopicUuid,
    partition: AssignedTopicPartition,
    batches: Vec<FetchBatch>,
}

impl ShareFetchDeliveryPartition {
    pub(super) const fn new(
        topic: Arc<str>,
        topic_uuid: ShareTopicUuid,
        partition: AssignedTopicPartition,
        batches: Vec<FetchBatch>,
    ) -> Self {
        Self {
            topic,
            topic_uuid,
            partition,
            batches,
        }
    }

    pub(in crate::consumer) fn topic(&self) -> &str {
        &self.topic
    }

    pub(in crate::consumer) const fn topic_uuid(&self) -> ShareTopicUuid {
        self.topic_uuid
    }

    pub(in crate::consumer) const fn partition(&self) -> AssignedTopicPartition {
        self.partition
    }

    pub(in crate::consumer) fn batches(&self) -> &[FetchBatch] {
        &self.batches
    }
}

/// One complete response-wide byte and broker-lock capability.
#[must_use = "a share delivery must be acknowledged or returned to its exact session owner"]
pub(in crate::consumer) struct ShareFetchDelivery {
    fence: ShareFetchSessionFence,
    partitions: Vec<ShareFetchDeliveryPartition>,
    acquisitions: Vec<ShareAcquisition>,
}

impl ShareFetchDelivery {
    fn from_staged(staged: StagedShareFetchDelivery, acquisitions: Vec<ShareAcquisition>) -> Self {
        let StagedShareFetchDelivery {
            fence,
            route,
            throttle_time_ms: _throttle_time_ms,
            endpoints,
            partitions,
            acquisitions: _acquisitions,
        } = staged;
        route.accept();
        drop(endpoints);
        Self {
            fence,
            partitions,
            acquisitions,
        }
    }

    pub(in crate::consumer) const fn group_id(&self) -> GroupId {
        self.fence.group_id()
    }

    pub(in crate::consumer) const fn fence(&self) -> ShareFetchSessionFence {
        self.fence
    }

    pub(in crate::consumer) fn partitions(&self) -> &[ShareFetchDeliveryPartition] {
        &self.partitions
    }

    pub(in crate::consumer) fn acquisitions(&self) -> &[ShareAcquisition] {
        &self.acquisitions
    }

    pub(in crate::consumer) fn into_parts(
        self,
    ) -> (
        ShareFetchSessionFence,
        Vec<ShareFetchDeliveryPartition>,
        Vec<ShareAcquisition>,
    ) {
        (self.fence, self.partitions, self.acquisitions)
    }

    pub(in crate::consumer) fn restore(
        fence: ShareFetchSessionFence,
        partitions: Vec<ShareFetchDeliveryPartition>,
        acquisitions: Vec<ShareAcquisition>,
    ) -> Self {
        Self {
            fence,
            partitions,
            acquisitions,
        }
    }
}

impl ShareFetchSessionOwner {
    pub(super) const fn owns_delivery_fence(&self, delivery: ShareFetchSessionFence) -> bool {
        same_session_owner(self.machine.fence(), delivery)
    }

    pub(super) fn take_delivery(
        &mut self,
        now: Moment,
    ) -> Result<Option<ShareFetchDelivery>, ShareFetchDeliveryTransferError> {
        let Some(staged) = self.staged.as_ref() else {
            return Ok(None);
        };
        if staged.acquisitions == 0 {
            return Err(ShareFetchDeliveryTransferError::Empty);
        }
        let acquisitions = self
            .machine
            .ledger_mut()
            .claim_batch(staged.fence, staged.acquisitions, now)
            .map_err(ShareFetchDeliveryTransferError::Core)?;
        let staged = self
            .staged
            .take()
            .unwrap_or_else(|| unreachable!("validated staged share delivery"));
        Ok(Some(ShareFetchDelivery::from_staged(staged, acquisitions)))
    }

    pub(super) fn reclaim_delivery(
        &mut self,
        delivery: ShareFetchDelivery,
    ) -> Result<(), ShareFetchDeliveryReclaimError> {
        let (fence, partitions, acquisitions) = delivery.into_parts();
        if !self.owns_delivery_fence(fence) {
            return Err(ShareFetchDeliveryReclaimError::new(
                ShareFetchDeliveryReclaimErrorKind::SessionMismatch,
                ShareFetchDelivery::restore(fence, partitions, acquisitions),
            ));
        }
        match self.machine.ledger_mut().abandon_batch(acquisitions) {
            Ok(releases) => {
                drop(releases);
                drop(partitions);
                Ok(())
            }
            Err(error) => Err(ShareFetchDeliveryReclaimError::new(
                ShareFetchDeliveryReclaimErrorKind::Core(error.kind()),
                ShareFetchDelivery::restore(fence, partitions, error.into_acquisitions()),
            )),
        }
    }
}

const fn same_session_owner(
    owner: ShareFetchSessionFence,
    delivery: ShareFetchSessionFence,
) -> bool {
    owner.broker_id().get() == delivery.broker_id().get()
        && owner.group_id().get() == delivery.group_id().get()
        && owner.member_id().get() == delivery.member_id().get()
        && owner.member_epoch().get() == delivery.member_epoch().get()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchDeliveryTransferError {
    Empty,
    Core(ShareAcquisitionAdmissionErrorKind),
}

#[must_use = "a rejected share delivery remains owned by its exact caller"]
pub(super) struct ShareFetchDeliveryReclaimError {
    kind: ShareFetchDeliveryReclaimErrorKind,
    delivery: ShareFetchDelivery,
}

impl ShareFetchDeliveryReclaimError {
    const fn new(kind: ShareFetchDeliveryReclaimErrorKind, delivery: ShareFetchDelivery) -> Self {
        Self { kind, delivery }
    }

    pub(super) const fn kind(&self) -> ShareFetchDeliveryReclaimErrorKind {
        self.kind
    }

    pub(super) fn into_delivery(self) -> ShareFetchDelivery {
        self.delivery
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchDeliveryReclaimErrorKind {
    SessionMismatch,
    Core(ShareAcquisitionAdmissionErrorKind),
}
