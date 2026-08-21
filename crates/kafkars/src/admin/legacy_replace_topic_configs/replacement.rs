//! Stable ordered full snapshot for one legacy topic configuration replacement.

use super::LegacyTopicConfigEntry;

/// One topic and its complete caller-ordered legacy configuration snapshot.
///
/// Kafka treats omitted keys as deleted or reset. An empty entry list is
/// deliberately representable and means replacing the topic's explicit
/// configuration with an empty snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyTopicConfigReplacement {
    topic: String,
    entries: Vec<LegacyTopicConfigEntry>,
}

impl LegacyTopicConfigReplacement {
    /// Creates one inert full replacement; validation occurs at submission.
    pub fn new<I>(topic: impl Into<String>, entries: I) -> Self
    where
        I: IntoIterator<Item = LegacyTopicConfigEntry>,
    {
        Self {
            topic: topic.into(),
            entries: entries.into_iter().collect(),
        }
    }

    /// Returns the topic whose complete configuration snapshot will be replaced.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the complete replacement snapshot in caller order.
    pub fn entries(&self) -> &[LegacyTopicConfigEntry] {
        &self.entries
    }

    pub(crate) fn into_parts(self) -> (String, Vec<LegacyTopicConfigEntry>) {
        (self.topic, self.entries)
    }
}
