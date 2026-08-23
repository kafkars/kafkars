//! Lossless bounded share registration and capture-first membership start.

use std::sync::Arc;

use kafka_client_core::GroupId;

use crate::clock::DeadlineCapture;

use super::{
    entry::{ShareConsumerEntry, ShareConsumerEntryBuildError},
    registry::{
        SHARE_CONSUMER_CAPACITY, SHARE_CONSUMER_RETAINED_NAME_BYTES, ShareConsumerRegistry,
    },
};

#[must_use = "share registration rejection retains every caller-owned name"]
pub(in crate::consumer) struct ShareConsumerRegistrationFailure {
    pub(in crate::consumer) kind: ShareConsumerRegistrationFailureKind,
    pub(in crate::consumer) group: Arc<str>,
    pub(in crate::consumer) rack: Option<Arc<str>>,
    pub(in crate::consumer) topics: Vec<Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerRegistrationFailureKind {
    Closed,
    Capacity,
    RetainedBytes,
    IdentityExhausted,
    InvalidInput,
    Allocation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerStartError {
    Closed,
    UnknownConsumer,
    AlreadyStarted,
}

impl ShareConsumerRegistry {
    pub(in crate::consumer) fn try_register(
        &mut self,
        group: Arc<str>,
        rack: Option<Arc<str>>,
        topics: Vec<Arc<str>>,
    ) -> Result<GroupId, ShareConsumerRegistrationFailure> {
        if !self.accepting {
            return Err(failure(
                ShareConsumerRegistrationFailureKind::Closed,
                group,
                rack,
                topics,
            ));
        }
        if self.entries.len() == SHARE_CONSUMER_CAPACITY {
            return Err(failure(
                ShareConsumerRegistrationFailureKind::Capacity,
                group,
                rack,
                topics,
            ));
        }
        let Some(group_id) = self.next_group_id else {
            return Err(failure(
                ShareConsumerRegistrationFailureKind::IdentityExhausted,
                group,
                rack,
                topics,
            ));
        };
        let entry = match ShareConsumerEntry::try_new(group_id, group, rack, topics) {
            Ok(entry) => entry,
            Err(build) => {
                let kind = match build.kind {
                    ShareConsumerEntryBuildError::Allocation
                    | ShareConsumerEntryBuildError::MemberIdentity => {
                        ShareConsumerRegistrationFailureKind::Allocation
                    }
                    ShareConsumerEntryBuildError::IdentityExhausted => {
                        ShareConsumerRegistrationFailureKind::IdentityExhausted
                    }
                    ShareConsumerEntryBuildError::EmptyGroup
                    | ShareConsumerEntryBuildError::EmptyRack
                    | ShareConsumerEntryBuildError::EmptySubscription
                    | ShareConsumerEntryBuildError::EmptyTopic
                    | ShareConsumerEntryBuildError::NameTooLong
                    | ShareConsumerEntryBuildError::TopicCapacity
                    | ShareConsumerEntryBuildError::DuplicateTopic => {
                        ShareConsumerRegistrationFailureKind::InvalidInput
                    }
                };
                return Err(failure(kind, build.group, build.rack, build.topics));
            }
        };
        let retained = entry.retained_name_bytes();
        let Some(next_retained) = self.retained_name_bytes.checked_add(retained) else {
            return Err(failure_from_entry(
                ShareConsumerRegistrationFailureKind::RetainedBytes,
                entry.into_registration_parts(),
            ));
        };
        if next_retained > SHARE_CONSUMER_RETAINED_NAME_BYTES {
            return Err(failure_from_entry(
                ShareConsumerRegistrationFailureKind::RetainedBytes,
                entry.into_registration_parts(),
            ));
        }
        self.next_group_id = group_id
            .get()
            .checked_add(1)
            .and_then(GroupId::try_from_raw);
        self.retained_name_bytes = next_retained;
        self.entries.push(entry);
        Ok(group_id)
    }

    pub(in crate::consumer) fn try_begin(
        &mut self,
        group_id: GroupId,
        capture: DeadlineCapture,
    ) -> Result<(), ShareConsumerStartError> {
        if !self.accepting {
            return Err(ShareConsumerStartError::Closed);
        }
        let entry = self
            .entry_mut(group_id)
            .ok_or(ShareConsumerStartError::UnknownConsumer)?;
        entry
            .begin(capture)
            .map_err(|()| ShareConsumerStartError::AlreadyStarted)
    }
}

fn failure(
    kind: ShareConsumerRegistrationFailureKind,
    group: Arc<str>,
    rack: Option<Arc<str>>,
    topics: Vec<Arc<str>>,
) -> ShareConsumerRegistrationFailure {
    ShareConsumerRegistrationFailure {
        kind,
        group,
        rack,
        topics,
    }
}

fn failure_from_entry(
    kind: ShareConsumerRegistrationFailureKind,
    (group, rack, topics): (Arc<str>, Option<Arc<str>>, Vec<Arc<str>>),
) -> ShareConsumerRegistrationFailure {
    failure(kind, group, rack, topics)
}
