//! Linear driver call retained for one share subscription topic identity.

use std::sync::Arc;

use kafka_client_core::TopicId;

use crate::{clock::OperationDeadline, driver::TopicPartitionCountCall};

/// One accepted immutable topic-view lookup owned by a share member.
#[must_use = "an accepted share topic lookup must settle or recover"]
pub(super) struct ShareTopicIdentityCall {
    pub(super) local_topic_id: TopicId,
    pub(super) topic: Arc<str>,
    pub(super) deadline: OperationDeadline,
    pub(super) call: TopicPartitionCountCall,
}
