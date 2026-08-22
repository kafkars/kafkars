//! Exact topic preparation, result validation, and best-effort cleanup.

use std::{
    io, thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kafkars::{Admin, BatchResult, Client, KafkaError, NewTopic, RetryAdvice, TopicDescription};

use super::{OPERATION_TIMEOUT, TestError, wait_within};

pub(crate) fn ready_client(client: &Client) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        match wait_within(client.ready(), "Client readiness")? {
            Ok(()) => return Ok(()),
            Err(error)
                if error.retry_advice() == RetryAdvice::RetrySafe && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(crate) fn create_topics(admin: &Admin, topics: &[String]) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "CreateTopics admission remained backpressured",
            )
            .into());
        }
        let requested = topics
            .iter()
            .map(|topic| NewTopic::new(topic, 1).replication_factor(1));
        let created = wait_within(
            admin
                .create_topics(requested)
                .deadline_after(deadline.saturating_duration_since(now))
                .submit(),
            "CreateTopics",
        )?;
        match created {
            Ok(created) => {
                require_success(created, topics, "CreateTopics")?;
                break;
            }
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
    let expected = topics
        .iter()
        .map(|topic| (topic.as_str(), 1_usize))
        .collect::<Vec<_>>();
    wait_for_topic_metadata(admin, &expected)
}

pub(crate) fn wait_for_topic_metadata(
    admin: &Admin,
    expected: &[(&str, usize)],
) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "created topics did not expose every expected partition leader",
            )
            .into());
        }
        let remaining = deadline.saturating_duration_since(now);
        let described = wait_within(
            admin
                .describe_topics(expected.iter().map(|(topic, _)| *topic))
                .deadline_after(remaining)
                .submit(),
            "created-topic metadata readiness",
        )?;
        match described {
            Ok(result) => {
                if topic_metadata_ready(result, expected)? {
                    return Ok(());
                }
            }
            Err(error)
                if topic_metadata_pending(&error)
                    || error.retry_advice() == RetryAdvice::RetrySafe => {}
            Err(error) => return Err(error.into()),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

pub(crate) fn delete_topics(admin: &Admin, topics: &[String]) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "DeleteTopics admission remained backpressured",
            )
            .into());
        }
        let deleted = wait_within(
            admin
                .delete_topics(topics.iter().cloned())
                .deadline_after(deadline.saturating_duration_since(now))
                .submit(),
            "DeleteTopics",
        )?;
        match deleted {
            Ok(result) => return require_success(result, topics, "DeleteTopics"),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
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

fn topic_metadata_ready(
    result: BatchResult<String, TopicDescription>,
    expected: &[(&str, usize)],
) -> Result<bool, TestError> {
    let entries = result.into_entries();
    if entries.len() != expected.len() {
        return Err(io::Error::other("DescribeTopics readiness changed topic count").into());
    }
    for ((topic, outcome), (expected_topic, expected_partitions)) in
        entries.into_iter().zip(expected)
    {
        if topic != *expected_topic {
            return Err(io::Error::other("DescribeTopics readiness changed topic order").into());
        }
        let description = match outcome {
            Ok(description) => description,
            Err(error) if topic_metadata_pending(&error) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if description.name() != *expected_topic
            || description.partitions().len() != *expected_partitions
        {
            return Ok(false);
        }
        for partition in description.partitions() {
            if let Some(error) = partition.error() {
                if topic_metadata_pending(error) {
                    return Ok(false);
                }
                return Err(error.clone().into());
            }
            if partition.leader_id().is_none() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn topic_metadata_pending(error: &KafkaError) -> bool {
    matches!(error.broker_code(), Some(3 | 5))
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
