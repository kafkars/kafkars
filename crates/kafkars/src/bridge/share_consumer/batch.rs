//! Private zero-copy translation over one linear engine share delivery.

use kafka_client_engine::share::{
    ShareConsumerBatch as EngineBatch, ShareConsumerHeader as EngineHeader,
    ShareConsumerRecord as EngineRecord, ShareConsumerRecords as EngineRecords,
};

/// Private linear bridge retaining one exact share delivery capability.
pub(crate) struct ShareConsumerBatch {
    inner: EngineBatch,
}

impl ShareConsumerBatch {
    pub(super) const fn from_engine(inner: EngineBatch) -> Self {
        Self { inner }
    }

    pub(crate) fn record_count(&self) -> usize {
        self.inner.record_count()
    }

    pub(crate) fn partition_count(&self) -> usize {
        self.inner.partition_count()
    }

    pub(crate) fn acquisition_count(&self) -> usize {
        self.inner.acquisition_count()
    }

    pub(crate) fn records(&self) -> ShareConsumerRecords<'_> {
        ShareConsumerRecords {
            inner: self.inner.records(),
        }
    }
}

impl std::fmt::Debug for ShareConsumerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumerBatch")
            .field("partition_count", &self.partition_count())
            .field("acquisition_count", &self.acquisition_count())
            .field("record_count", &self.record_count())
            .finish()
    }
}

/// Private iterator translating engine share record views without copying.
#[derive(Debug)]
pub(crate) struct ShareConsumerRecords<'batch> {
    inner: EngineRecords<'batch>,
}

impl<'batch> Iterator for ShareConsumerRecords<'batch> {
    type Item = ShareConsumerRecord<'batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|inner| ShareConsumerRecord { inner })
    }
}

/// Private borrowed share record translation.
#[derive(Debug)]
pub(crate) struct ShareConsumerRecord<'batch> {
    inner: EngineRecord<'batch>,
}

impl ShareConsumerRecord<'_> {
    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub(crate) const fn partition(&self) -> u32 {
        self.inner.partition()
    }

    pub(crate) const fn offset(&self) -> i64 {
        self.inner.offset()
    }

    pub(crate) const fn delivery_count(&self) -> i16 {
        self.inner.delivery_count()
    }

    pub(crate) const fn timestamp_millis(&self) -> Option<i64> {
        self.inner.timestamp_millis()
    }

    pub(crate) fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }

    pub(crate) fn headers(&self) -> impl ExactSizeIterator<Item = ShareConsumerHeader<'_>> {
        self.inner
            .headers()
            .map(|inner| ShareConsumerHeader { inner })
    }
}

/// Private borrowed share-header translation.
#[derive(Debug)]
pub(crate) struct ShareConsumerHeader<'record> {
    inner: EngineHeader<'record>,
}

impl ShareConsumerHeader<'_> {
    pub(crate) fn key(&self) -> &[u8] {
        self.inner.key()
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }
}
