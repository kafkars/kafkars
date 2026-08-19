//! Private facade translation over one engine-owned group-consumer lease.

use kafka_client_engine::{
    GroupConsumerBatch as EngineBatch, GroupConsumerCheckpointBuilder as EngineCheckpointBuilder,
    GroupConsumerCheckpointMarkErrorKind as EngineCheckpointMarkErrorKind,
    GroupConsumerHeader as EngineHeader, GroupConsumerRecord as EngineRecord,
    GroupConsumerRecords as EngineRecords,
};

use super::group_consumer_checkpoint::GroupConsumerCheckpoint;

/// Linear private bridge retaining one exact group Fetch delivery lease.
pub(crate) struct GroupConsumerBatch {
    inner: EngineBatch,
}

impl GroupConsumerBatch {
    pub(crate) const fn from_engine(inner: EngineBatch) -> Self {
        Self { inner }
    }

    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub(crate) fn partition(&self) -> i32 {
        self.inner.partition()
    }

    pub(crate) fn checkpoint_next_offset(&self) -> i64 {
        self.inner.checkpoint_next_offset()
    }

    pub(crate) fn record_count(&self) -> usize {
        self.inner.record_count()
    }

    pub(crate) fn records(&self) -> GroupConsumerRecords<'_> {
        GroupConsumerRecords {
            inner: self.inner.records(),
        }
    }

    pub(crate) fn checkpoint(self) -> GroupConsumerCheckpoint {
        GroupConsumerCheckpoint::from_engine(self.inner.checkpoint())
    }

    pub(crate) fn checkpoint_builder(&self) -> GroupConsumerCheckpointBuilder<'_> {
        GroupConsumerCheckpointBuilder {
            inner: self.inner.checkpoint_builder(),
        }
    }
}

/// Private exact-batch prefix-checkpoint translation.
pub(crate) struct GroupConsumerCheckpointBuilder<'batch> {
    inner: EngineCheckpointBuilder<'batch>,
}

impl GroupConsumerCheckpointBuilder<'_> {
    pub(crate) fn mark_processed(
        &mut self,
        record: &GroupConsumerRecord<'_>,
    ) -> Result<(), GroupConsumerCheckpointMarkErrorKind> {
        self.inner
            .mark_processed(&record.inner)
            .map_err(|error| match error.kind() {
                EngineCheckpointMarkErrorKind::ForeignRecord => {
                    GroupConsumerCheckpointMarkErrorKind::ForeignRecord
                }
                EngineCheckpointMarkErrorKind::OutOfOrder => {
                    GroupConsumerCheckpointMarkErrorKind::OutOfOrder
                }
            })
    }

    pub(crate) fn finish(self) -> GroupConsumerCheckpoint {
        GroupConsumerCheckpoint::from_engine(self.inner.finish())
    }
}

impl std::fmt::Debug for GroupConsumerCheckpointBuilder<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("GroupConsumerCheckpointBuilder")
            .field(&self.inner)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerCheckpointMarkErrorKind {
    ForeignRecord,
    OutOfOrder,
}

impl std::fmt::Debug for GroupConsumerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerBatch")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("checkpoint_next_offset", &self.checkpoint_next_offset())
            .field("record_count", &self.record_count())
            .finish()
    }
}

/// Private iterator translating engine group record views without copying.
#[derive(Debug)]
pub(crate) struct GroupConsumerRecords<'batch> {
    inner: EngineRecords<'batch>,
}

impl<'batch> Iterator for GroupConsumerRecords<'batch> {
    type Item = GroupConsumerRecord<'batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|inner| GroupConsumerRecord { inner })
    }
}

/// Private borrowed group record translation.
#[derive(Debug)]
pub(crate) struct GroupConsumerRecord<'batch> {
    inner: EngineRecord<'batch>,
}

impl GroupConsumerRecord<'_> {
    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub(crate) fn partition(&self) -> i32 {
        self.inner.partition()
    }

    pub(crate) fn offset(&self) -> i64 {
        self.inner.offset()
    }

    pub(crate) fn timestamp_millis(&self) -> Option<i64> {
        self.inner.timestamp_millis()
    }

    pub(crate) fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }

    pub(crate) fn headers(&self) -> impl ExactSizeIterator<Item = GroupConsumerHeader<'_>> {
        self.inner
            .headers()
            .map(|inner| GroupConsumerHeader { inner })
    }
}

/// Private borrowed group-header translation.
#[derive(Debug)]
pub(crate) struct GroupConsumerHeader<'record> {
    inner: EngineHeader<'record>,
}

impl GroupConsumerHeader<'_> {
    pub(crate) fn key(&self) -> &[u8] {
        self.inner.key()
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }
}
