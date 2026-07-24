//! Stable topic configuration query prepared before `DescribeConfigs` submission.

/// One topic and its optional ordered configuration-key selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigQuery {
    topic: String,
    configuration_keys: Option<Vec<String>>,
}

impl TopicConfigQuery {
    /// Requests every configuration for one topic.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            configuration_keys: None,
        }
    }

    /// Restricts the response to the supplied configuration keys in this order.
    #[must_use]
    pub fn configuration_keys<I, T>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.configuration_keys = Some(keys.into_iter().map(Into::into).collect());
        self
    }

    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns `None` for all keys or the exact requested key order.
    pub fn selected_configuration_keys(&self) -> Option<&[String]> {
        self.configuration_keys.as_deref()
    }

    pub(crate) fn into_parts(self) -> (String, Option<Vec<String>>) {
        (self.topic, self.configuration_keys)
    }
}

impl From<String> for TopicConfigQuery {
    fn from(topic: String) -> Self {
        Self::new(topic)
    }
}

impl From<&str> for TopicConfigQuery {
    fn from(topic: &str) -> Self {
        Self::new(topic)
    }
}
