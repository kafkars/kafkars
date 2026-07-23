//! Allocation-free policy view over one driver-owned immutable topic view.

use core::fmt;

use crate::PartitionIndex;

use super::{AvailablePartition, PartitionCount, PartitionSelection, TopicMetadataGeneration};

/// Lazy normalized access to one immutable topology-authority topic view.
///
/// Scalar methods and indexed entries must describe one coherent immutable
/// generation for the duration of [`TopicPartitionFacts::new`].
/// Implementations must keep `available_at(0..available_len())` stable and in
/// strictly increasing partition order. A future engine adapter can implement
/// this trait over a borrowed driver view without copying its entry types.
pub trait TopicPartitionSource {
    /// Returns the immutable metadata generation.
    fn generation(&self) -> TopicMetadataGeneration;

    /// Returns the total logical count, including partitions without known leaders.
    fn logical_count(&self) -> PartitionCount;

    /// Returns the number of partitions with currently known leaders.
    fn available_len(&self) -> usize;

    /// Returns one available partition by canonical-order index.
    fn available_at(&self, index: usize) -> Option<AvailablePartition>;
}

/// Borrowed access to normalized topic facts without retaining a second cache.
#[derive(Clone, Copy)]
pub struct TopicPartitionFacts<'a> {
    source: &'a dyn TopicPartitionSource,
    generation: TopicMetadataGeneration,
    logical_count: PartitionCount,
    available_len: usize,
}

impl<'a> TopicPartitionFacts<'a> {
    /// Captures one coherent scalar snapshot while borrowing its immutable entries.
    pub fn new(source: &'a dyn TopicPartitionSource) -> Self {
        Self {
            source,
            generation: source.generation(),
            logical_count: source.logical_count(),
            available_len: source.available_len(),
        }
    }

    pub(super) const fn generation(self) -> TopicMetadataGeneration {
        self.generation
    }

    pub(super) const fn logical_count(self) -> PartitionCount {
        self.logical_count
    }

    pub(super) const fn available_len(self) -> usize {
        self.available_len
    }

    pub(super) fn available_at(
        self,
        index: usize,
    ) -> Result<AvailablePartition, TopicPartitionFactsError> {
        let declared_len = self.available_len();
        if index >= declared_len {
            return Err(TopicPartitionFactsError::AvailableIndexOutsideSet {
                index,
                declared_len,
            });
        }
        let fact = self.source.available_at(index).ok_or(
            TopicPartitionFactsError::AvailableIndexMissing {
                index,
                declared_len,
            },
        )?;
        self.validate_partition(fact.partition())?;
        Ok(fact)
    }

    pub(super) fn find_available(
        self,
        partition: PartitionIndex,
    ) -> Result<Option<AvailablePartition>, TopicPartitionFactsError> {
        self.validate_partition(partition)?;
        let mut start = 0;
        let mut end = self.available_len();
        while start < end {
            let middle = start + (end - start) / 2;
            let fact = self.available_at(middle)?;
            match fact.partition().cmp(&partition) {
                core::cmp::Ordering::Less => start = middle + 1,
                core::cmp::Ordering::Equal => return Ok(Some(fact)),
                core::cmp::Ordering::Greater => end = middle,
            }
        }
        Ok(None)
    }

    pub(super) fn select(
        self,
        partition: PartitionIndex,
    ) -> Result<PartitionSelection, TopicPartitionFactsError> {
        Ok(match self.find_available(partition)? {
            Some(fact) => PartitionSelection::available(self.generation(), fact),
            None => PartitionSelection::unavailable(self.generation(), partition),
        })
    }

    pub(super) fn select_available(self, fact: AvailablePartition) -> PartitionSelection {
        PartitionSelection::available(self.generation(), fact)
    }

    fn validate_partition(self, partition: PartitionIndex) -> Result<(), TopicPartitionFactsError> {
        let logical_count = self.logical_count();
        if partition.get() >= logical_count.get() {
            Err(TopicPartitionFactsError::PartitionOutsideTopic {
                partition,
                logical_count,
            })
        } else {
            Ok(())
        }
    }
}

impl fmt::Debug for TopicPartitionFacts<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicPartitionFacts")
            .field("generation", &self.generation)
            .field("logical_count", &self.logical_count)
            .field("available_len", &self.available_len)
            .finish_non_exhaustive()
    }
}

/// Rejection of an incoherent lazy topic fact source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicPartitionFactsError {
    /// A partition lies outside the declared logical partition range.
    PartitionOutsideTopic {
        /// Rejected partition index.
        partition: PartitionIndex,
        /// Declared total logical partition count.
        logical_count: PartitionCount,
    },
    /// An indexed lookup exceeded the source's declared available length.
    AvailableIndexOutsideSet {
        /// Rejected available-set index.
        index: usize,
        /// Declared available-set length.
        declared_len: usize,
    },
    /// The source omitted an index below its declared available length.
    AvailableIndexMissing {
        /// Missing available-set index.
        index: usize,
        /// Declared available-set length.
        declared_len: usize,
    },
}

impl fmt::Display for TopicPartitionFactsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartitionOutsideTopic {
                partition,
                logical_count,
            } => write!(
                formatter,
                "partition {} is outside logical count {}",
                partition.get(),
                logical_count.get()
            ),
            Self::AvailableIndexOutsideSet {
                index,
                declared_len,
            } => write!(
                formatter,
                "available index {index} is outside declared length {declared_len}"
            ),
            Self::AvailableIndexMissing {
                index,
                declared_len,
            } => write!(
                formatter,
                "available index {index} is missing below declared length {declared_len}"
            ),
        }
    }
}

impl std::error::Error for TopicPartitionFactsError {}
