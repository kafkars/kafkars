//! Exact caller-ordered `TxnOffsetCommit` target ownership and borrowing.

use std::sync::Arc;

use crate::protocol::transaction::TransactionOffsetCommitRef;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionOffsetCommitTarget {
    topic: Arc<str>,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<Arc<str>>,
}

impl TransactionOffsetCommitTarget {
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

    fn as_ref(&self) -> TransactionOffsetCommitRef<'_> {
        TransactionOffsetCommitRef::new(
            &self.topic,
            self.partition,
            self.next_offset,
            self.leader_epoch,
            self.metadata.as_deref(),
        )
    }
}

pub(super) fn target_refs(
    targets: &[TransactionOffsetCommitTarget],
) -> Option<Vec<TransactionOffsetCommitRef<'_>>> {
    let mut refs = Vec::new();
    refs.try_reserve_exact(targets.len()).ok()?;
    refs.extend(targets.iter().map(TransactionOffsetCommitTarget::as_ref));
    Some(refs)
}
