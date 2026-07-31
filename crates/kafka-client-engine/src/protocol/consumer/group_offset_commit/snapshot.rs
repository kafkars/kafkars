//! Lossless construction from pre-reserved prepared-entry and result capacity.

use std::sync::Arc;

use kafka_client_core::GroupOffsetCommitEffect;

use crate::clock::OperationDeadline;

use super::{
    entry_reservation::GroupOffsetCommitEntryReservation,
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
        entry_reservation: GroupOffsetCommitEntryReservation,
        result_reservation: GroupOffsetCommitResultReservation,
    ) -> Result<Self, GroupOffsetCommitPreparationError> {
        Self::prepare(
            effect,
            operation_deadline,
            session,
            topic_names,
            entry_reservation,
            result_reservation,
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
        entry_reservation: GroupOffsetCommitEntryReservation,
        result_reservation: GroupOffsetCommitResultReservation,
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
                    entry_reservation,
                    result_reservation,
                ));
            }
        };
        if entry_reservation.entry_count() != entry_count {
            return Err(GroupOffsetCommitPreparationError::new(
                GroupOffsetCommitPreparationErrorKind::EntryReservationMismatch {
                    entries: entry_count,
                    reserved: entry_reservation.entry_count(),
                },
                effect,
                operation_deadline,
                session,
                topic_names,
                entry_reservation,
                result_reservation,
            ));
        }
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
                entry_reservation,
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
                entry_reservation,
                result_reservation,
            ));
        };
        let (operation_id, entries) =
            match materialize_entries(&effect, &topic_names, entry_reservation) {
                Ok(materialized) => materialized,
                Err((kind, entry_reservation)) => {
                    return Err(GroupOffsetCommitPreparationError::new(
                        kind,
                        effect,
                        operation_deadline,
                        session,
                        topic_names,
                        entry_reservation,
                        result_reservation,
                    ));
                }
            };
        let requires_leader_epoch = entries.iter().any(|entry| entry.leader_epoch.is_some());
        let requires_consumer_group_version = session.consumer_group_protocol;
        Ok(Self::new(
            operation_id,
            operation_deadline,
            session.group,
            session.member,
            classic_generation,
            entries,
            result_reservation.into_outcomes(),
            requires_leader_epoch,
            requires_consumer_group_version,
        ))
    }
}

fn materialize_entries(
    effect: &GroupOffsetCommitEffect,
    topic_names: &[GroupOffsetCommitTopicName],
    entry_reservation: GroupOffsetCommitEntryReservation,
) -> Result<
    (
        kafka_client_core::OperationId,
        Vec<PreparedGroupOffsetCommitEntry>,
    ),
    (
        GroupOffsetCommitPreparationErrorKind,
        GroupOffsetCommitEntryReservation,
    ),
> {
    let entry_count = entry_reservation.entry_count();
    let mut entries = entry_reservation.into_entries();
    let GroupOffsetCommitEffect::Submit {
        operation_id,
        checkpoint,
        ..
    } = effect
    else {
        return Err((
            GroupOffsetCommitPreparationErrorKind::UnexpectedEffect,
            GroupOffsetCommitEntryReservation::recover_group_offset_commit_entries(
                entry_count,
                entries,
            ),
        ));
    };
    for entry in checkpoint.entries() {
        let Some(topic) = topic_names
            .iter()
            .find(|candidate| candidate.topic_id == entry.topic_id())
        else {
            return Err((
                GroupOffsetCommitPreparationErrorKind::UnknownTopic(entry.topic_id()),
                GroupOffsetCommitEntryReservation::recover_group_offset_commit_entries(
                    entry_count,
                    entries,
                ),
            ));
        };
        let Ok(partition_index) = i32::try_from(entry.partition().get()) else {
            return Err((
                GroupOffsetCommitPreparationErrorKind::PartitionOutOfRange {
                    topic_id: entry.topic_id(),
                    partition: entry.partition(),
                },
                GroupOffsetCommitEntryReservation::recover_group_offset_commit_entries(
                    entry_count,
                    entries,
                ),
            ));
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
