//! Exact topic preparation, result validation, and best-effort cleanup.

use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};

use kafkars::{Admin, BatchResult, Client, NewTopic};

use super::{OPERATION_TIMEOUT, TestError, wait_within};

pub(crate) fn ready_client(client: &Client) -> Result<(), TestError> {
    wait_within(client.ready(), "Client readiness")??;
    Ok(())
}

pub(crate) fn create_topics(admin: &Admin, topics: &[String]) -> Result<(), TestError> {
    let requested = topics
        .iter()
        .map(|topic| NewTopic::new(topic, 1).replication_factor(1));
    let created = wait_within(
        admin
            .create_topics(requested)
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "CreateTopics",
    )??;
    require_success(created, topics, "CreateTopics")
}

pub(crate) fn delete_topics(admin: &Admin, topics: &[String]) -> Result<(), TestError> {
    let deleted = wait_within(
        admin
            .delete_topics(topics.iter().cloned())
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "DeleteTopics",
    )??;
    require_success(deleted, topics, "DeleteTopics")
}

fn require_success(
    result: BatchResult<String, ()>,
    expected: &[String],
    operation: &str,
) -> Result<(), TestError> {
    let entries = result.into_entries();
    if entries.len() != expected.len() {
        return Err(io::Error::other(format!(
            "{operation} returned {} entries instead of {}",
            entries.len(),
            expected.len()
        ))
        .into());
    }
    for ((topic, outcome), expected_topic) in entries.into_iter().zip(expected) {
        if &topic != expected_topic {
            return Err(io::Error::other(format!(
                "{operation} returned topic {topic:?} instead of {expected_topic:?}"
            ))
            .into());
        }
        outcome?;
    }
    Ok(())
}

pub(crate) fn unique_name(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{timestamp}", std::process::id())
}

pub(crate) struct TopicCleanup {
    admin: Admin,
    topics: Option<Vec<String>>,
}

impl TopicCleanup {
    pub(crate) const fn new(admin: Admin, topics: Vec<String>) -> Self {
        Self {
            admin,
            topics: Some(topics),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.topics = None;
    }
}

impl Drop for TopicCleanup {
    fn drop(&mut self) {
        let Some(topics) = self.topics.take() else {
            return;
        };
        let _cleanup = delete_topics(&self.admin, &topics);
    }
}
