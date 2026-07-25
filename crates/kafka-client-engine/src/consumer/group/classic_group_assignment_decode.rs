//! Pure catalog canonicalization of one bounded normalized Sync assignment.

use kafka_client_core::{GroupAssignmentPartition, PartitionIndex, TopicId};

use crate::protocol::consumer::{CLASSIC_SYNC_MAX_MEMBER_PARTITIONS, NamedAssignmentPartition};

use super::{
    classic_group_candidate::ClassicGroupCycleCandidate, session_catalog::GroupSessionCatalog,
};

/// Scalar reason one normalized assignment could not enter core policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupAssignmentDecodeError {
    CatalogChanged,
    PartitionCapacity {
        actual: usize,
        limit: usize,
    },
    Allocation {
        requested: usize,
        limit: usize,
    },
    UnknownTopic {
        entry: usize,
    },
    UnsubscribedTopic {
        entry: usize,
        topic_id: TopicId,
    },
    NegativePartition {
        entry: usize,
        partition: i32,
    },
    DuplicatePartition {
        topic_id: TopicId,
        partition: PartitionIndex,
    },
}

/// Lossless rejection retaining the protocol-normalized assignment owner.
#[must_use = "a rejected normalized assignment remains owned"]
pub(super) struct ClassicGroupAssignmentDecodeFailure {
    kind: ClassicGroupAssignmentDecodeError,
    partitions: Vec<NamedAssignmentPartition>,
}

impl ClassicGroupAssignmentDecodeFailure {
    pub(super) const fn kind(&self) -> ClassicGroupAssignmentDecodeError {
        self.kind
    }

    pub(super) fn partitions(&self) -> &[NamedAssignmentPartition] {
        &self.partitions
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ClassicGroupAssignmentDecodeError,
        Vec<NamedAssignmentPartition>,
    ) {
        (self.kind, self.partitions)
    }
}

/// Converts protocol-owned spellings into ordered core-owned scalar identities.
pub(super) fn decode_classic_group_assignment(
    catalog: &GroupSessionCatalog,
    candidate: &ClassicGroupCycleCandidate,
    partitions: Vec<NamedAssignmentPartition>,
) -> Result<Vec<GroupAssignmentPartition>, ClassicGroupAssignmentDecodeFailure> {
    match decode_borrowed(catalog, candidate, &partitions) {
        Ok(decoded) => Ok(decoded),
        Err(kind) => Err(ClassicGroupAssignmentDecodeFailure { kind, partitions }),
    }
}

fn decode_borrowed(
    catalog: &GroupSessionCatalog,
    candidate: &ClassicGroupCycleCandidate,
    partitions: &[NamedAssignmentPartition],
) -> Result<Vec<GroupAssignmentPartition>, ClassicGroupAssignmentDecodeError> {
    if !candidate.matches_catalog_base(catalog) {
        return Err(ClassicGroupAssignmentDecodeError::CatalogChanged);
    }
    if partitions.len() > CLASSIC_SYNC_MAX_MEMBER_PARTITIONS {
        return Err(ClassicGroupAssignmentDecodeError::PartitionCapacity {
            actual: partitions.len(),
            limit: CLASSIC_SYNC_MAX_MEMBER_PARTITIONS,
        });
    }
    let mut decoded = reserve_assignment(partitions.len())?;
    for (entry, partition) in partitions.iter().enumerate() {
        let topic_id = catalog
            .topic_id(partition.topic())
            .ok_or(ClassicGroupAssignmentDecodeError::UnknownTopic { entry })?;
        if !candidate.local_owns_topic(topic_id) {
            return Err(ClassicGroupAssignmentDecodeError::UnsubscribedTopic { entry, topic_id });
        }
        let raw = u32::try_from(partition.partition()).map_err(|_error| {
            ClassicGroupAssignmentDecodeError::NegativePartition {
                entry,
                partition: partition.partition(),
            }
        })?;
        decoded.push(GroupAssignmentPartition::new(
            topic_id,
            PartitionIndex::from_raw(raw),
        ));
    }
    decoded.sort_unstable();
    if let Some(repeated) = decoded.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(ClassicGroupAssignmentDecodeError::DuplicatePartition {
            topic_id: repeated[0].topic_id(),
            partition: repeated[0].partition(),
        });
    }
    Ok(decoded)
}

fn reserve_assignment(
    requested: usize,
) -> Result<Vec<GroupAssignmentPartition>, ClassicGroupAssignmentDecodeError> {
    let mut decoded = Vec::new();
    decoded.try_reserve_exact(requested).map_err(|_error| {
        ClassicGroupAssignmentDecodeError::Allocation {
            requested,
            limit: CLASSIC_SYNC_MAX_MEMBER_PARTITIONS,
        }
    })?;
    Ok(decoded)
}
