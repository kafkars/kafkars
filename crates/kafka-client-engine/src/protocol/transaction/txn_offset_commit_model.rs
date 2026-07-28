//! Borrowed group and next-offset facts for transactional wire adaptation.

/// Borrowed group membership identity for one transactional offset transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionGroupIdentityRef<'a> {
    group_id: &'a str,
    generation_id_or_member_epoch: i32,
    member_id: &'a str,
    group_instance_id: Option<&'a str>,
}

impl<'a> TransactionGroupIdentityRef<'a> {
    pub(crate) const fn new(
        group_id: &'a str,
        generation_id_or_member_epoch: i32,
        member_id: &'a str,
        group_instance_id: Option<&'a str>,
    ) -> Self {
        Self {
            group_id,
            generation_id_or_member_epoch,
            member_id,
            group_instance_id,
        }
    }

    pub(crate) const fn group_id(self) -> &'a str {
        self.group_id
    }

    pub(crate) const fn generation_id_or_member_epoch(self) -> i32 {
        self.generation_id_or_member_epoch
    }

    pub(crate) const fn member_id(self) -> &'a str {
        self.member_id
    }

    pub(crate) const fn group_instance_id(self) -> Option<&'a str> {
        self.group_instance_id
    }
}

/// One caller-owned next offset borrowed during generated-wire adaptation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionOffsetCommitRef<'a> {
    topic: &'a str,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<&'a str>,
}

impl<'a> TransactionOffsetCommitRef<'a> {
    pub(crate) const fn new(
        topic: &'a str,
        partition: i32,
        next_offset: i64,
        leader_epoch: Option<i32>,
        metadata: Option<&'a str>,
    ) -> Self {
        Self {
            topic,
            partition,
            next_offset,
            leader_epoch,
            metadata,
        }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }

    pub(crate) const fn next_offset(self) -> i64 {
        self.next_offset
    }

    pub(crate) const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }

    pub(crate) const fn metadata(self) -> Option<&'a str> {
        self.metadata
    }
}
