//! Pre-start catalog and protocol preparation for one position bootstrap.

use std::sync::Arc;

use kafka_client_core::{
    GroupId, GroupPositionBootstrapBuildErrorKind, GroupPositionBootstrapEffect,
    GroupPositionBootstrapInput, GroupPositionBootstrapMachine, GroupPositionBootstrapMachineError,
    GroupPositionBootstrapTerminal, GroupPositionPartitionFact, LiveGroupAssignment,
    MembershipCycle, Moment, PartitionIndex, TopicId,
};

use crate::{
    clock::OperationDeadline,
    driver::GroupPositionOffsetFetchKey,
    protocol::consumer::{
        GroupOffsetFetchRequestPreparationFailure, prepare_group_offset_fetch_request,
    },
};

use super::{
    super::session_catalog::{GroupSessionCatalog, GroupSessionCatalogError},
    ClassicGroupPositionCompleted, ClassicGroupPositionPrepared,
    preparation_input::{
        RequiredProtocol, copy_core_partitions, prepare_protocol_topics, require_protocol_shape,
        reserve_result_buffer,
    },
};

/// Maximum generated request storage retained by one position bootstrap.
pub(in crate::consumer::group) const CLASSIC_GROUP_POSITION_REQUEST_RETAINED_BYTES: usize =
    64 * 1024;

/// Maximum normalized response storage retained by one position bootstrap.
pub(in crate::consumer::group) const CLASSIC_GROUP_POSITION_RESULT_RETAINED_BYTES: usize =
    4 * 1024 * 1024;

/// Prepared RPC ownership or an already completed empty-assignment bootstrap.
#[must_use = "position preparation must be installed into the execution owner"]
#[expect(
    clippy::large_enum_variant,
    reason = "both variants retain exact preallocated owners; boxing would add hidden allocation"
)]
pub(in crate::consumer::group) enum ClassicGroupPositionPreparation {
    Prepared(ClassicGroupPositionPrepared),
    Complete(ClassicGroupPositionCompleted),
}

/// An exact disagreement between protocol preparation and core start policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupPositionPreparationMismatch {
    ProtocolRequestForEmptyAssignment,
    ProtocolNoRequestForAssignedPartitions,
    MissingCoreEffect,
    FetchForEmptyAssignment,
    CompletionForAssignedPartitions,
    FetchFence,
    FetchDeadline,
    FetchPartitions,
    CompletionFence,
    CompletionDeadline,
    EmptyAssignmentTerminal,
}

/// Local failure before any position RPC can be admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupPositionPreparationError {
    CatalogGroup {
        catalog: GroupId,
        assignment: GroupId,
    },
    AssignmentCopyAllocation,
    ResultBufferAllocation,
    TopicListAllocation,
    TopicPartitionListAllocation(TopicId),
    UnknownTopic(GroupSessionCatalogError),
    PartitionOutOfRange(PartitionIndex),
    Protocol(GroupOffsetFetchRequestPreparationFailure),
    CoreBuild(GroupPositionBootstrapBuildErrorKind),
    CoreStart(GroupPositionBootstrapMachineError),
    Mismatch(ClassicGroupPositionPreparationMismatch),
}

/// Prepares protocol ownership before starting one deterministic core machine.
pub(in crate::consumer::group) fn prepare_classic_group_position(
    catalog: &GroupSessionCatalog,
    cycle: MembershipCycle,
    assignment: &LiveGroupAssignment,
    operation_deadline: OperationDeadline,
    now: Moment,
) -> Result<ClassicGroupPositionPreparation, ClassicGroupPositionPreparationError> {
    let fence = kafka_client_core::GroupPositionFence::new(
        assignment.group_id(),
        cycle,
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    if catalog.group_id() != assignment.group_id() {
        return Err(ClassicGroupPositionPreparationError::CatalogGroup {
            catalog: catalog.group_id(),
            assignment: assignment.group_id(),
        });
    }
    let partitions = copy_core_partitions(assignment.partitions())?;
    let topics = prepare_protocol_topics(catalog, &partitions)?;
    let protocol = prepare_group_offset_fetch_request(
        Arc::clone(catalog.group()),
        topics,
        CLASSIC_GROUP_POSITION_REQUEST_RETAINED_BYTES,
    )
    .map_err(ClassicGroupPositionPreparationError::Protocol)?;
    let protocol = require_protocol_shape(protocol, partitions.is_empty())?;
    let result_buffer = reserve_result_buffer(partitions.len())?;

    let mut machine =
        GroupPositionBootstrapMachine::try_new(fence, operation_deadline.core(), partitions)
            .map_err(|error| ClassicGroupPositionPreparationError::CoreBuild(error.kind()))?;
    let transition = machine
        .apply(GroupPositionBootstrapInput::Start { fence, now })
        .map_err(|error| ClassicGroupPositionPreparationError::CoreStart(error.kind()))?;
    finish_preparation(
        machine,
        protocol,
        fence,
        operation_deadline,
        now,
        result_buffer,
        transition.into_effect(),
    )
}

fn finish_preparation(
    machine: GroupPositionBootstrapMachine,
    protocol: RequiredProtocol,
    fence: kafka_client_core::GroupPositionFence,
    operation_deadline: OperationDeadline,
    observed_at: Moment,
    result_buffer: Vec<GroupPositionPartitionFact>,
    effect: Option<GroupPositionBootstrapEffect>,
) -> Result<ClassicGroupPositionPreparation, ClassicGroupPositionPreparationError> {
    match (protocol, effect) {
        (
            RequiredProtocol::Prepared(prepared),
            Some(GroupPositionBootstrapEffect::FetchOffsets {
                fence: effect_fence,
                deadline,
                partitions,
            }),
        ) => {
            if effect_fence != fence {
                return mismatch(ClassicGroupPositionPreparationMismatch::FetchFence);
            }
            if deadline != operation_deadline.core() {
                return mismatch(ClassicGroupPositionPreparationMismatch::FetchDeadline);
            }
            if partitions.as_slice() != machine.partitions() {
                return mismatch(ClassicGroupPositionPreparationMismatch::FetchPartitions);
            }
            let (correlation, request) = prepared.into_parts();
            Ok(ClassicGroupPositionPreparation::Prepared(
                ClassicGroupPositionPrepared::new(
                    GroupPositionOffsetFetchKey::new(fence, operation_deadline),
                    machine,
                    correlation,
                    request,
                    result_buffer,
                ),
            ))
        }
        (
            RequiredProtocol::NoRequest,
            Some(GroupPositionBootstrapEffect::Complete {
                fence: effect_fence,
                deadline,
                terminal,
            }),
        ) => {
            if effect_fence != fence {
                return mismatch(ClassicGroupPositionPreparationMismatch::CompletionFence);
            }
            if deadline != operation_deadline.core() {
                return mismatch(ClassicGroupPositionPreparationMismatch::CompletionDeadline);
            }
            if !matches!(
                &terminal,
                GroupPositionBootstrapTerminal::Ready(batch) if batch.facts().is_empty()
            ) {
                return mismatch(ClassicGroupPositionPreparationMismatch::EmptyAssignmentTerminal);
            }
            Ok(ClassicGroupPositionPreparation::Complete(
                ClassicGroupPositionCompleted::new(machine, terminal, observed_at),
            ))
        }
        (RequiredProtocol::NoRequest, Some(GroupPositionBootstrapEffect::FetchOffsets { .. })) => {
            mismatch(ClassicGroupPositionPreparationMismatch::FetchForEmptyAssignment)
        }
        (RequiredProtocol::Prepared(_), Some(GroupPositionBootstrapEffect::Complete { .. })) => {
            mismatch(ClassicGroupPositionPreparationMismatch::CompletionForAssignedPartitions)
        }
        (_, None) => mismatch(ClassicGroupPositionPreparationMismatch::MissingCoreEffect),
    }
}

fn mismatch<T>(
    mismatch: ClassicGroupPositionPreparationMismatch,
) -> Result<T, ClassicGroupPositionPreparationError> {
    Err(ClassicGroupPositionPreparationError::Mismatch(mismatch))
}
