//! Exact producer-record transfer retaining any upstream consumer byte lease.

use std::sync::Arc;

use bytes::Bytes;

use super::{Header, Record};
use crate::{TopicUuid, header_name::SourceOwner};

/// Crate-private ownership transfer for focused record evidence.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct RecordParts {
    pub(crate) topic: Arc<str>,
    pub(crate) partition: Option<i32>,
    pub(crate) timestamp_milliseconds: Option<i64>,
    pub(crate) key: Option<Bytes>,
    pub(crate) value: Option<Bytes>,
    pub(crate) headers: Vec<Header>,
}

/// Crate-private exact transfer retaining any upstream consumer byte lease.
pub(crate) struct RecordTransferParts {
    pub(crate) topic: Arc<str>,
    pub(crate) expected_topic_uuid: Option<TopicUuid>,
    pub(crate) partition: Option<i32>,
    pub(crate) timestamp_milliseconds: Option<i64>,
    pub(crate) key: Option<Bytes>,
    pub(crate) value: Option<Bytes>,
    pub(crate) headers: Vec<Header>,
    pub(crate) source_owner: SourceOwner,
}

impl Record {
    #[cfg(test)]
    pub(crate) fn from_parts(parts: RecordParts) -> Self {
        Self {
            topic: parts.topic,
            expected_topic_uuid: None,
            partition: parts.partition,
            timestamp_milliseconds: parts.timestamp_milliseconds,
            key: parts.key,
            value: parts.value,
            headers: parts.headers,
            source_owner: SourceOwner::none(),
        }
    }

    pub(crate) fn from_transfer_parts(parts: RecordTransferParts) -> Self {
        Self {
            topic: parts.topic,
            expected_topic_uuid: parts.expected_topic_uuid,
            partition: parts.partition,
            timestamp_milliseconds: parts.timestamp_milliseconds,
            key: parts.key,
            value: parts.value,
            headers: parts.headers,
            source_owner: parts.source_owner,
        }
    }

    pub(crate) fn shared_source_owner(&self) -> Option<Arc<dyn Send + Sync>> {
        self.source_owner.shared_arc()
    }
}
