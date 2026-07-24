//! Private facade translation over one engine-owned assigned-consumer lease.

use kafka_client_engine::{
    AssignedConsumerBatch as EngineBatch, AssignedConsumerHeader as EngineHeader,
    AssignedConsumerRecord as EngineRecord, AssignedConsumerRecords as EngineRecords,
};

/// Linear private bridge retaining one exact engine delivery lease.
pub(crate) struct AssignedConsumerBatch {
    inner: EngineBatch,
}

impl AssignedConsumerBatch {
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

    pub(crate) fn records(&self) -> AssignedConsumerRecords<'_> {
        AssignedConsumerRecords {
            inner: self.inner.records(),
        }
    }
}

impl std::fmt::Debug for AssignedConsumerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerBatch")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("checkpoint_next_offset", &self.checkpoint_next_offset())
            .field("record_count", &self.record_count())
            .finish()
    }
}

/// Private iterator translating engine record views without copying bytes.
#[derive(Debug)]
pub(crate) struct AssignedConsumerRecords<'batch> {
    inner: EngineRecords<'batch>,
}

impl<'batch> Iterator for AssignedConsumerRecords<'batch> {
    type Item = AssignedConsumerRecord<'batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|inner| AssignedConsumerRecord { inner })
    }
}

/// Private borrowed record translation.
#[derive(Debug)]
pub(crate) struct AssignedConsumerRecord<'batch> {
    inner: EngineRecord<'batch>,
}

impl AssignedConsumerRecord<'_> {
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

    pub(crate) fn headers(&self) -> impl ExactSizeIterator<Item = AssignedConsumerHeader<'_>> {
        self.inner
            .headers()
            .map(|inner| AssignedConsumerHeader { inner })
    }
}

/// Private borrowed header translation.
#[derive(Debug)]
pub(crate) struct AssignedConsumerHeader<'record> {
    inner: EngineHeader<'record>,
}

impl AssignedConsumerHeader<'_> {
    pub(crate) fn key(&self) -> &[u8] {
        self.inner.key()
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }
}
