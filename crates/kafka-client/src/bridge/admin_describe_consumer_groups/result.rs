//! Exhaustive stable translation of concrete engine group-description outcomes.

use std::time::Duration;

use kafka_client_engine::{
    ConsumerGroupAssignment as EngineAssignment, ConsumerGroupBrokerError as EngineBrokerError,
    ConsumerGroupDescription as EngineDescription,
    ConsumerGroupDescriptionDetails as EngineDescriptionDetails,
    ConsumerGroupDescriptionError as EngineDescriptionError,
    ConsumerGroupDescriptionMember as EngineMember,
    ConsumerGroupMemberDetails as EngineMemberDetails,
    ConsumerGroupTopicPartitions as EngineTopicPartitions, DescribeConsumerGroupsAcceptedFaultKind,
    DescribeConsumerGroupsAdmissionError, DescribeConsumerGroupsAdmissionErrorKind,
    DescribeConsumerGroupsDeliveryStatus, DescribeConsumerGroupsFailure,
    DescribeConsumerGroupsFailureKind, DescribeConsumerGroupsObserverError,
    DescribeConsumerGroupsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{
        BatchResult, ClassicConsumerGroupDetails, ClassicConsumerGroupMemberDetails,
        ConsumerGroupAssignment, ConsumerGroupDescription, ConsumerGroupDescriptionDetails,
        ConsumerGroupMember, ConsumerGroupMemberDetails, ConsumerGroupTopicPartitions,
        ConsumerProtocolGroupDetails, ConsumerProtocolMemberDetails, DescribeConsumerGroupsResult,
    },
};

use super::operation::AdminDescribeConsumerGroupsResult;

pub(super) fn translate_admission_error(error: DescribeConsumerGroupsAdmissionError) -> KafkaError {
    let kind = error.kind();
    let public = match kind {
        DescribeConsumerGroupsAdmissionErrorKind::InvalidRequest
        | DescribeConsumerGroupsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        DescribeConsumerGroupsAdmissionErrorKind::Contended
        | DescribeConsumerGroupsAdmissionErrorKind::Capacity
        | DescribeConsumerGroupsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        DescribeConsumerGroupsAdmissionErrorKind::Closed => ErrorKind::State,
        DescribeConsumerGroupsAdmissionErrorKind::IdentityExhausted
        | DescribeConsumerGroupsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("DescribeConsumerGroups admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(
    fault: DescribeConsumerGroupsAcceptedFaultKind,
) -> KafkaError {
    match fault {
        DescribeConsumerGroupsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeConsumerGroups was accepted but its host wake failed",
        ),
        DescribeConsumerGroupsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeConsumerGroups was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<DescribeConsumerGroupsOutcome, DescribeConsumerGroupsObserverError>,
) -> AdminDescribeConsumerGroupsResult {
    match result {
        Ok(DescribeConsumerGroupsOutcome::Groups(batch)) => {
            let (throttle_time_ms, groups) = batch.into_parts();
            let entries = groups
                .into_iter()
                .map(|group| {
                    let (group_id, result) = group.into_parts();
                    (
                        group_id,
                        result
                            .map(translate_description)
                            .map_err(translate_description_error),
                    )
                })
                .collect();
            Ok(DescribeConsumerGroupsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(DescribeConsumerGroupsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_description(description: EngineDescription) -> ConsumerGroupDescription {
    let (state, details, members, operations) = description.into_parts();
    ConsumerGroupDescription::new(
        state,
        match details {
            EngineDescriptionDetails::Classic(details) => {
                let (protocol_type, protocol_data) = details.into_parts();
                ConsumerGroupDescriptionDetails::Classic(ClassicConsumerGroupDetails::new(
                    protocol_type,
                    protocol_data,
                ))
            }
            EngineDescriptionDetails::Consumer(details) => {
                let (group_epoch, assignment_epoch, assignor_name) = details.into_parts();
                ConsumerGroupDescriptionDetails::Consumer(ConsumerProtocolGroupDetails::new(
                    group_epoch,
                    assignment_epoch,
                    assignor_name,
                ))
            }
        },
        members.into_iter().map(translate_member).collect(),
        operations,
    )
}

fn translate_member(member: EngineMember) -> ConsumerGroupMember {
    let (member_id, group_instance_id, client_id, client_host, details) = member.into_parts();
    ConsumerGroupMember::new(
        member_id,
        group_instance_id,
        client_id,
        client_host,
        match details {
            EngineMemberDetails::Classic(details) => {
                let (metadata, assignment) = details.into_parts();
                ConsumerGroupMemberDetails::Classic(ClassicConsumerGroupMemberDetails::new(
                    metadata, assignment,
                ))
            }
            EngineMemberDetails::Consumer(details) => {
                let (
                    rack_id,
                    member_epoch,
                    subscriptions,
                    subscription_regex,
                    assignment,
                    target_assignment,
                    member_type,
                ) = details.into_parts();
                ConsumerGroupMemberDetails::Consumer(ConsumerProtocolMemberDetails::new(
                    rack_id,
                    member_epoch,
                    subscriptions,
                    subscription_regex,
                    translate_assignment(assignment),
                    translate_assignment(target_assignment),
                    member_type,
                ))
            }
        },
    )
}

fn translate_assignment(assignment: EngineAssignment) -> ConsumerGroupAssignment {
    ConsumerGroupAssignment::new(
        assignment
            .into_topics()
            .into_iter()
            .map(translate_topic_partitions)
            .collect(),
    )
}

fn translate_topic_partitions(topic: EngineTopicPartitions) -> ConsumerGroupTopicPartitions {
    let (topic_id, topic_name, partitions) = topic.into_parts();
    ConsumerGroupTopicPartitions::new(topic_id, topic_name, partitions)
}

fn translate_description_error(error: EngineDescriptionError) -> KafkaError {
    match error {
        EngineDescriptionError::Broker(error) => translate_group_error(error),
        EngineDescriptionError::Operation(error) => translate_failure(error),
    }
}

fn translate_group_error(error: EngineBrokerError) -> KafkaError {
    let (code, message, truncated) = error.into_parts();
    let detail = message.map_or_else(
        || format!("Kafka returned consumer-group describe broker code {code}"),
        |message| format!("Kafka returned consumer-group describe broker code {code}: {message}"),
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(truncated)
}

fn translate_failure(failure: DescribeConsumerGroupsFailure) -> KafkaError {
    let kind = failure.kind();
    let public = match kind {
        DescribeConsumerGroupsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        DescribeConsumerGroupsFailureKind::DriverRejected
        | DescribeConsumerGroupsFailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        DescribeConsumerGroupsFailureKind::Transport => ErrorKind::Transport,
        DescribeConsumerGroupsFailureKind::Compatibility => ErrorKind::Compatibility,
        DescribeConsumerGroupsFailureKind::InvalidResponse => ErrorKind::Broker,
        DescribeConsumerGroupsFailureKind::NotAttempted => ErrorKind::State,
    };
    KafkaError::new(public, format!("DescribeConsumerGroups failed: {kind:?}"))
        .with_delivery_status(translate_delivery(failure.delivery()))
}

const fn translate_delivery(delivery: DescribeConsumerGroupsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        DescribeConsumerGroupsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        DescribeConsumerGroupsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: DescribeConsumerGroupsObserverError) -> KafkaError {
    let public = match error {
        DescribeConsumerGroupsObserverError::AlreadyObserved => ErrorKind::State,
        DescribeConsumerGroupsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
