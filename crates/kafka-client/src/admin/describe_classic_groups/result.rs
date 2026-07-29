//! Caller-ordered classic-group results translated from the shared terminal.

use std::time::Duration;

use crate::{
    BatchResult, ErrorKind, KafkaError,
    admin::{
        ConsumerGroupDescription, ConsumerGroupDescriptionDetails, ConsumerGroupMember,
        ConsumerGroupMemberDetails, DescribeConsumerGroupsResult,
    },
};

use super::{ClassicGroupDescription, ClassicGroupMember};

/// Successful deterministic `DescribeClassicGroups` operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClassicGroupsResult {
    throttle_time: Duration,
    groups: BatchResult<String, ClassicGroupDescription>,
}

impl DescribeClassicGroupsResult {
    pub(crate) fn from_consumer(result: DescribeConsumerGroupsResult) -> Self {
        let throttle_time = result.throttle_time();
        let groups = result
            .into_groups()
            .into_entries()
            .into_iter()
            .map(|(group_id, outcome)| (group_id, outcome.and_then(translate_classic_description)))
            .collect();
        Self {
            throttle_time,
            groups: BatchResult::new(groups),
        }
    }

    /// Returns the maximum broker throttle observed across coordinator calls.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns exact per-group outcomes in caller order.
    pub const fn groups(&self) -> &BatchResult<String, ClassicGroupDescription> {
        &self.groups
    }

    /// Consumes this result into caller-ordered per-group outcomes.
    pub fn into_groups(self) -> BatchResult<String, ClassicGroupDescription> {
        self.groups
    }
}

fn translate_classic_description(
    description: ConsumerGroupDescription,
) -> Result<ClassicGroupDescription, KafkaError> {
    let (protocol_type, protocol_data) = match description.details() {
        ConsumerGroupDescriptionDetails::Classic(details) => (
            details.protocol_type().to_owned(),
            details.protocol_data().to_owned(),
        ),
        ConsumerGroupDescriptionDetails::Consumer(_) => {
            return Err(modern_description_error());
        }
    };
    let members = description
        .members()
        .iter()
        .map(translate_classic_member)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ClassicGroupDescription::new(
        description.state().to_owned(),
        protocol_type,
        protocol_data,
        members,
        description.authorized_operations(),
    ))
}

fn translate_classic_member(
    member: &ConsumerGroupMember,
) -> Result<ClassicGroupMember, KafkaError> {
    let (metadata, assignment) = match member.details() {
        ConsumerGroupMemberDetails::Classic(details) => {
            (details.metadata().to_vec(), details.assignment().to_vec())
        }
        ConsumerGroupMemberDetails::Consumer(_) => return Err(modern_member_error()),
    };
    Ok(ClassicGroupMember::new(
        member.member_id().to_owned(),
        member.group_instance_id().map(str::to_owned),
        member.client_id().to_owned(),
        member.client_host().to_owned(),
        metadata,
        assignment,
    ))
}

fn modern_description_error() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "DescribeClassicGroups received a modern description on its ClassicOnly path",
    )
}

fn modern_member_error() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "DescribeClassicGroups received a modern member on its ClassicOnly path",
    )
}
