//! Lossless construction of one prepared commit from pre-reserved result capacity.

use std::sync::Arc;

use kafka_client_core::GroupOffsetCommitEffect;

use crate::clock::OperationDeadline;

use super::{
    model::{PreparedGroupOffsetCommit, PreparedGroupOffsetCommitEntry},
    preparation::{GroupOffsetCommitPreparationError, GroupOffsetCommitPreparationErrorKind},
    result_reservation::GroupOffsetCommitResultReservation,
    session::{ClassicGroupCommitSession, GroupOffsetCommitTopicName},
    validation::validate_group_offset_commit_inputs,
};

impl PreparedGroupOffsetCommit {
    #[allow(
        clippy::result_large_err,
        reason = "preparation failure must return every exact linear input owner"
    )]
    pub(crate) fn from_effect(
        effect: GroupOffsetCommitEffect,
        operation_deadline: OperationDeadline,
        session: ClassicGroupCommitSession,
        topic_names: Vec<GroupOffsetCommitTopicName>,
        result_reservation: GroupOffsetCommitResultReservation,
    ) -> Result<Self, GroupOffsetCommitPreparationError> {
        Self::prepare(
            effect,
            operation_deadline,
            session,
            topic_names,
            result_reservation,
            None,
        )
    }

    #[cfg(test)]
    #[allow(
        clippy::result_large_err,
        reason = "preparation failure must return every exact linear input owner"
    )]
    pub(super) fn from_effect_with_entry_reservation_for_test(
        effect: GroupOffsetCommitEffect,
        operation_deadline: OperationDeadline,
        session: ClassicGroupCommitSession,
        topic_names: Vec<GroupOffsetCommitTopicName>,
        result_reservation: GroupOffsetCommitResultReservation,
        entry_reservation: usize,
    ) -> Result<Self, GroupOffsetCommitPreparationError> {
        Self::prepare(
            effect,
            operation_deadline,
            session,
            topic_names,
            result_reservation,
            Some(entry_reservation),
        )
    }

    #[allow(
        clippy::result_large_err,
        reason = "preparation failure must return every exact linear input owner"
    )]
    fn prepare(
        effect: GroupOffsetCommitEffect,
        operation_deadline: OperationDeadline,
        session: ClassicGroupCommitSession,
        topic_names: Vec<GroupOffsetCommitTopicName>,
        result_reservation: GroupOffsetCommitResultReservation,
        entry_reservation_override: Option<usize>,
    ) -> Result<Self, GroupOffsetCommitPreparationError> {
        let entry_count = match validate_group_offset_commit_inputs(
            &effect,
            operation_deadline,
            &session,
            &topic_names,
        ) {
            Ok(entry_count) => entry_count,
            Err(kind) => {
                return Err(GroupOffsetCommitPreparationError::new(
                    kind,
                    effect,
                    operation_deadline,
                    session,
                    topic_names,
                    result_reservation,
                ));
            }
        };
        if result_reservation.entry_count() != entry_count {
            return Err(GroupOffsetCommitPreparationError::new(
                GroupOffsetCommitPreparationErrorKind::ResultReservationMismatch {
                    entries: entry_count,
                    reserved: result_reservation.entry_count(),
                },
                effect,
                operation_deadline,
                session,
                topic_names,
                result_reservation,
            ));
        }
        let Ok(classic_generation) = i32::try_from(session.classic_generation) else {
            return Err(GroupOffsetCommitPreparationError::new(
                GroupOffsetCommitPreparationErrorKind::ClassicGenerationOutOfRange,
                effect,
                operation_deadline,
                session,
                topic_names,
                result_reservation,
            ));
        };
        let entry_reservation = entry_reservation_override.unwrap_or(entry_count);
        let (operation_id, entries) =
            match materialize_entries(&effect, &topic_names, entry_reservation) {
                Ok(materialized) => materialized,
                Err(kind) => {
                    return Err(GroupOffsetCommitPreparationError::new(
                        kind,
                        effect,
                        operation_deadline,
                        session,
                        topic_names,
                        result_reservation,
                    ));
                }
            };
        let requires_leader_epoch = entries.iter().any(|entry| entry.leader_epoch.is_some());
        Ok(Self::new(
            operation_id,
            operation_deadline,
            session.group,
            session.member,
            classic_generation,
            entries,
            result_reservation.into_outcomes(),
            requires_leader_epoch,
        ))
    }
}

fn materialize_entries(
    effect: &GroupOffsetCommitEffect,
    topic_names: &[GroupOffsetCommitTopicName],
    entry_reservation: usize,
) -> Result<
    (
        kafka_client_core::OperationId,
        Vec<PreparedGroupOffsetCommitEntry>,
    ),
    GroupOffsetCommitPreparationErrorKind,
> {
    let GroupOffsetCommitEffect::Submit {
        operation_id,
        checkpoint,
        ..
    } = effect
    else {
        return Err(GroupOffsetCommitPreparationErrorKind::UnexpectedEffect);
    };
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(entry_reservation)
        .map_err(|_| GroupOffsetCommitPreparationErrorKind::AllocationFailed)?;
    for entry in checkpoint.entries() {
        let Some(topic) = topic_names
            .iter()
            .find(|candidate| candidate.topic_id == entry.topic_id())
        else {
            return Err(GroupOffsetCommitPreparationErrorKind::UnknownTopic(
                entry.topic_id(),
            ));
        };
        let Ok(partition_index) = i32::try_from(entry.partition().get()) else {
            return Err(GroupOffsetCommitPreparationErrorKind::PartitionOutOfRange {
                topic_id: entry.topic_id(),
                partition: entry.partition(),
            });
        };
        entries.push(PreparedGroupOffsetCommitEntry {
            topic_id: entry.topic_id(),
            topic: Arc::clone(&topic.name),
            partition: entry.partition(),
            partition_index,
            next_offset: entry.next_offset(),
            leader_epoch: entry.leader_epoch(),
        });
    }
    Ok((*operation_id, entries))
}
