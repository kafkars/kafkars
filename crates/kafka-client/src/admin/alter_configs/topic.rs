//! Stable ordered topic change set prepared before public submission.

use super::ConfigAlteration;

/// One topic and its caller-ordered incremental configuration changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigAlterations {
    topic: String,
    alterations: Vec<ConfigAlteration>,
}

impl TopicConfigAlterations {
    /// Creates one inert change set; validation occurs at submission.
    pub fn new<I>(topic: impl Into<String>, alterations: I) -> Self
    where
        I: IntoIterator<Item = ConfigAlteration>,
    {
        Self {
            topic: topic.into(),
            alterations: alterations.into_iter().collect(),
        }
    }

    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns changes in caller order.
    pub fn alterations(&self) -> &[ConfigAlteration] {
        &self.alterations
    }

    pub(crate) fn into_parts(self) -> (String, Vec<ConfigAlteration>) {
        (self.topic, self.alterations)
    }
}
