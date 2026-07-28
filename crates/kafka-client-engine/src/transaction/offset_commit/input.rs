//! Exact owned group, offset, and transaction-fence input.

use std::sync::Arc;

use kafka_client_core::{GroupPositionFence, TransactionEpoch, TransactionalOwnerId};

use crate::clock::OperationDeadline;

/// Exact classic-group membership spelling and assignment fence.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionOffsetCommitGroup {
    group_id: Arc<str>,
    generation_id: i32,
    member_id: Arc<str>,
    group_instance_id: Option<Arc<str>>,
    fence: GroupPositionFence,
}

impl TransactionOffsetCommitGroup {
    pub(crate) const fn new(
        group_id: Arc<str>,
        generation_id: i32,
        member_id: Arc<str>,
        group_instance_id: Option<Arc<str>>,
        fence: GroupPositionFence,
    ) -> Self {
        Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            fence,
        }
    }

    pub(super) fn group_id(&self) -> &str {
        &self.group_id
    }

    pub(super) const fn generation_id(&self) -> i32 {
        self.generation_id
    }

    pub(super) fn member_id(&self) -> &str {
        &self.member_id
    }

    pub(super) fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    pub(super) const fn fence(&self) -> GroupPositionFence {
        self.fence
    }

    pub(super) fn retained_bytes(&self) -> Option<usize> {
        self.group_id
            .len()
            .checked_add(self.member_id.len())?
            .checked_add(
                self.group_instance_id
                    .as_ref()
                    .map_or(0, |identity| identity.len()),
            )
    }
}

/// One caller-ordered next offset and nullable metadata.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionOffsetCommitOffset {
    topic: Arc<str>,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<Arc<str>>,
}

impl TransactionOffsetCommitOffset {
    pub(crate) const fn new(
        topic: Arc<str>,
        partition: i32,
        next_offset: i64,
        leader_epoch: Option<i32>,
        metadata: Option<Arc<str>>,
    ) -> Self {
        Self {
            topic,
            partition,
            next_offset,
            leader_epoch,
            metadata,
        }
    }

    pub(super) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    pub(super) const fn partition(&self) -> i32 {
        self.partition
    }

    pub(super) const fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub(super) const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    pub(super) fn metadata(&self) -> Option<&Arc<str>> {
        self.metadata.as_ref()
    }

    fn retained_bytes(&self) -> Option<usize> {
        self.topic
            .len()
            .checked_add(self.metadata.as_ref().map_or(0, |metadata| metadata.len()))
    }
}

/// One exact transactional offset transfer captured at its public boundary.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionOffsetCommitRequest {
    owner_id: TransactionalOwnerId,
    epoch: TransactionEpoch,
    group: TransactionOffsetCommitGroup,
    offsets: Vec<TransactionOffsetCommitOffset>,
    deadline: OperationDeadline,
}

impl TransactionOffsetCommitRequest {
    pub(crate) const fn new(
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
        group: TransactionOffsetCommitGroup,
        offsets: Vec<TransactionOffsetCommitOffset>,
        deadline: OperationDeadline,
    ) -> Self {
        Self {
            owner_id,
            epoch,
            group,
            offsets,
            deadline,
        }
    }

    pub(in crate::transaction) const fn owner_id(&self) -> TransactionalOwnerId {
        self.owner_id
    }

    pub(in crate::transaction) const fn epoch(&self) -> TransactionEpoch {
        self.epoch
    }

    pub(super) const fn group(&self) -> &TransactionOffsetCommitGroup {
        &self.group
    }

    pub(super) fn offsets(&self) -> &[TransactionOffsetCommitOffset] {
        &self.offsets
    }

    pub(super) const fn deadline(&self) -> OperationDeadline {
        self.deadline
    }

    pub(super) fn retained_bytes(&self) -> Option<usize> {
        self.offsets
            .iter()
            .try_fold(self.group.retained_bytes()?, |total, offset| {
                total.checked_add(offset.retained_bytes()?)
            })
    }
}
