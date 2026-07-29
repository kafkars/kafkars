//! Exhaustive stable translation of engine ShareGroup description outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        DescribeShareGroupResult as PublicResult, ShareGroupAssignment, ShareGroupDescription,
        ShareGroupMember, ShareGroupTopicPartitions,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Assignment, BrokerError,
        DeliveryStatus, Description, Failure, FailureKind, Member, ObserverError, Outcome,
        TopicAssignment,
    },
    operation::AdminDescribeShareGroupResult,
};

pub(in crate::bridge) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::IdentityExhausted | AdmissionErrorKind::HostUnavailable => {
            ErrorKind::Internal
        }
    };
    KafkaError::new(
        public,
        format!("DescribeShareGroup admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(in crate::bridge) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeShareGroup was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeShareGroup was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeShareGroupResult {
    match result {
        Ok(Outcome::Described(result)) => {
            let (throttle_time_ms, description) = result.into_parts();
            Ok(PublicResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                translate_description(description),
            ))
        }
        Ok(Outcome::BrokerRejected(error)) => Err(translate_broker_error(error)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Ok(Outcome::Batch(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "DescribeShareGroup received a batch terminal from its singular plan",
        )
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(in crate::bridge) fn translate_description(description: Description) -> ShareGroupDescription {
    let (
        group_id,
        state,
        group_epoch,
        assignment_epoch,
        assignor_name,
        members,
        authorized_operations,
    ) = description.into_parts();
    ShareGroupDescription::new(
        group_id,
        state,
        group_epoch,
        assignment_epoch,
        assignor_name,
        members.into_iter().map(translate_member).collect(),
        authorized_operations,
    )
}

fn translate_member(member: Member) -> ShareGroupMember {
    let (
        member_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscribed_topic_names,
        assignment,
    ) = member.into_parts();
    ShareGroupMember::new(
        member_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscribed_topic_names,
        translate_assignment(assignment),
    )
}

fn translate_assignment(assignment: Assignment) -> ShareGroupAssignment {
    ShareGroupAssignment::new(
        assignment
            .into_topics()
            .into_iter()
            .map(translate_topic_assignment)
            .collect(),
    )
}

fn translate_topic_assignment(topic: TopicAssignment) -> ShareGroupTopicPartitions {
    let (topic_id, topic_name, partitions) = topic.into_parts();
    ShareGroupTopicPartitions::new(topic_id, topic_name, partitions)
}

pub(in crate::bridge) fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
    let context = format!("Kafka rejected DescribeShareGroup after {throttle_time_ms} ms throttle");
    let diagnostic = match message {
        Some(message) => format!("{context} with broker code {code}: {message}"),
        None => format!("{context} with broker code {code}"),
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

pub(in crate::bridge) fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DescribeShareGroup failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DeliveryStatus) -> PublicDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => PublicDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => PublicDeliveryStatus::PossiblySent,
    }
}

pub(in crate::bridge) fn translate_observer_error(error: ObserverError) -> KafkaError {
    let public = match error {
        ObserverError::AlreadyObserved => ErrorKind::State,
        ObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
