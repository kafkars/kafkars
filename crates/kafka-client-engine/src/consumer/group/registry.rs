//! Sole bounded registry owner for private classic-group consumers.

use std::sync::Arc;

use kafka_client_core::GroupId;

use super::{
    offset_commit::GroupOffsetCommitHost,
    registry_entry::GroupConsumerEntry,
    session_catalog::{GroupSessionCatalogError, MAX_KAFKA_GROUP_STRING_BYTES},
};

pub(super) const GROUP_CONSUMER_CAPACITY: usize = 8;
pub(super) const GROUP_CONSUMER_RETAINED_NAME_BYTES: usize =
    GROUP_CONSUMER_CAPACITY * MAX_KAFKA_GROUP_STRING_BYTES;

/// Registration rejection retaining the exact caller-owned group spelling.
#[must_use = "group registration rejection retains the caller group spelling"]
pub(super) struct GroupConsumerRegistrationFailure {
    pub(super) kind: GroupConsumerRegistrationFailureKind,
    pub(super) group: Arc<str>,
}

/// Bounded local reason a group catalog could not be registered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerRegistrationFailureKind {
    Closed,
    Capacity,
    RetainedBytes,
    IdentityExhausted,
    Catalog(GroupSessionCatalogError),
}

/// Many bounded catalogs sharing one global bounded offset-commit host.
pub(crate) struct GroupConsumerRegistry {
    pub(super) entries: Vec<GroupConsumerEntry>,
    pub(super) next_group_id: Option<GroupId>,
    pub(super) retained_group_bytes: usize,
    pub(super) accepting: bool,
    pub(super) offset_commits: GroupOffsetCommitHost,
}

impl GroupConsumerRegistry {
    pub(crate) fn start() -> std::io::Result<Self> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(GROUP_CONSUMER_CAPACITY)
            .map_err(|_error| std::io::Error::other("group consumer entry reservation failed"))?;
        Ok(Self {
            entries,
            next_group_id: GroupId::try_from_raw(1),
            retained_group_bytes: 0,
            accepting: true,
            offset_commits: GroupOffsetCommitHost::start_group_offset_commit_host()?,
        })
    }

    pub(super) fn try_register(
        &mut self,
        group: Arc<str>,
    ) -> Result<GroupId, GroupConsumerRegistrationFailure> {
        if !self.accepting {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::Closed,
                group,
            ));
        }
        if self.entries.len() == GROUP_CONSUMER_CAPACITY {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::Capacity,
                group,
            ));
        }
        let Some(group_id) = self.next_group_id else {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::IdentityExhausted,
                group,
            ));
        };
        let entry = match GroupConsumerEntry::try_new(group_id, Arc::clone(&group)) {
            Ok(entry) => entry,
            Err(error) => {
                return Err(registration_failure(
                    GroupConsumerRegistrationFailureKind::Catalog(error),
                    group,
                ));
            }
        };
        let next_bytes = match self.retained_group_bytes.checked_add(entry.group_bytes()) {
            Some(bytes) if bytes <= GROUP_CONSUMER_RETAINED_NAME_BYTES => bytes,
            _ => {
                return Err(registration_failure(
                    GroupConsumerRegistrationFailureKind::RetainedBytes,
                    group,
                ));
            }
        };
        self.next_group_id = group_id
            .get()
            .checked_add(1)
            .and_then(GroupId::try_from_raw);
        self.retained_group_bytes = next_bytes;
        self.entries.push(entry);
        Ok(group_id)
    }

    pub(super) fn entry(&self, group_id: GroupId) -> Option<&GroupConsumerEntry> {
        self.entries
            .iter()
            .find(|entry| entry.group_id() == group_id)
    }

    pub(super) fn registered_group_count(&self) -> usize {
        self.entries.len()
    }

    pub(super) const fn retained_group_bytes(&self) -> usize {
        self.retained_group_bytes
    }
}

fn registration_failure(
    kind: GroupConsumerRegistrationFailureKind,
    group: Arc<str>,
) -> GroupConsumerRegistrationFailure {
    GroupConsumerRegistrationFailure { kind, group }
}
