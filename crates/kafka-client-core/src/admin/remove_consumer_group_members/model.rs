//! Validated semantic input for one static consumer-group member removal.

use core::fmt;
use std::collections::BTreeSet;

const MAX_KAFKA_STRING_BYTES: usize = i16::MAX as usize;

/// One caller-ordered static group member selected for removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMemberRemoval {
    group_instance_id: String,
}

impl ConsumerGroupMemberRemoval {
    /// Creates one static-member identity for enclosing-plan validation.
    pub const fn new(group_instance_id: String) -> Self {
        Self { group_instance_id }
    }

    /// Returns the exact static group-instance identity.
    pub fn group_instance_id(&self) -> &str {
        &self.group_instance_id
    }
}

/// Validated intent for one destructive member-removal request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveConsumerGroupMembersPlan {
    group_id: String,
    members: Vec<ConsumerGroupMemberRemoval>,
    reason: Option<String>,
}

impl RemoveConsumerGroupMembersPlan {
    /// Validates one group and a nonempty caller-ordered unique static-member set.
    pub fn new(
        group_id: String,
        members: Vec<ConsumerGroupMemberRemoval>,
        reason: Option<String>,
    ) -> Result<Self, RemoveConsumerGroupMembersPlanError> {
        validate_string(&group_id, false).map_err(|too_long| {
            if too_long {
                RemoveConsumerGroupMembersPlanError::GroupIdTooLong
            } else {
                RemoveConsumerGroupMembersPlanError::EmptyGroupId
            }
        })?;
        if members.is_empty() {
            return Err(RemoveConsumerGroupMembersPlanError::EmptyMemberBatch);
        }
        let mut identities = BTreeSet::new();
        for member in &members {
            validate_string(member.group_instance_id(), false).map_err(|too_long| {
                if too_long {
                    RemoveConsumerGroupMembersPlanError::GroupInstanceIdTooLong
                } else {
                    RemoveConsumerGroupMembersPlanError::EmptyGroupInstanceId
                }
            })?;
            if !identities.insert(member.group_instance_id()) {
                return Err(RemoveConsumerGroupMembersPlanError::DuplicateGroupInstanceId);
            }
        }
        if let Some(reason) = reason.as_deref() {
            validate_string(reason, true)
                .map_err(|_| RemoveConsumerGroupMembersPlanError::ReasonTooLong)?;
        }
        Ok(Self {
            group_id,
            members,
            reason,
        })
    }

    /// Returns the exact consumer-group coordinator key.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns static members in original caller order.
    pub fn members(&self) -> &[ConsumerGroupMemberRemoval] {
        &self.members
    }

    /// Returns the optional broker-visible removal reason.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

fn validate_string(value: &str, empty_allowed: bool) -> Result<(), bool> {
    if !empty_allowed && value.is_empty() {
        return Err(false);
    }
    if value.len() > MAX_KAFKA_STRING_BYTES {
        return Err(true);
    }
    Ok(())
}

/// Invalid deterministic consumer-group member-removal intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoveConsumerGroupMembersPlanError {
    /// The request must name one explicit consumer group.
    EmptyGroupId,
    /// The group identity cannot fit Kafka's string domain.
    GroupIdTooLong,
    /// The request must select at least one static member.
    EmptyMemberBatch,
    /// Static member identities must not be empty.
    EmptyGroupInstanceId,
    /// A static member identity cannot fit Kafka's string domain.
    GroupInstanceIdTooLong,
    /// One request cannot repeat a static member identity.
    DuplicateGroupInstanceId,
    /// The optional removal reason cannot fit Kafka's string domain.
    ReasonTooLong,
}

impl fmt::Display for RemoveConsumerGroupMembersPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyGroupId => "consumer group id is empty",
            Self::GroupIdTooLong => "consumer group id exceeds Kafka's string limit",
            Self::EmptyMemberBatch => "consumer group member-removal batch is empty",
            Self::EmptyGroupInstanceId => "consumer group instance id is empty",
            Self::GroupInstanceIdTooLong => {
                "consumer group instance id exceeds Kafka's string limit"
            }
            Self::DuplicateGroupInstanceId => {
                "consumer group member-removal batch contains a duplicate instance id"
            }
            Self::ReasonTooLong => "consumer group member-removal reason is too long",
        })
    }
}

impl std::error::Error for RemoveConsumerGroupMembersPlanError {}
