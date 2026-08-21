//! Self-cleaning replicated-topic fixtures and bounded polling for nightly cells.

use std::{io, thread, time::Instant};

use kafkars::{Admin, Client, ClientBuilder, NewTopic, TopicDescription};

use crate::real_broker_support::{
    OPERATION_TIMEOUT, TestError, client_builder_from_environment, delete_topics, ready_client,
    unique_name, wait_within,
};

pub(super) struct Fixture {
    pub(super) client: Client,
    pub(super) topic: String,
    cleaned: bool,
}

impl Fixture {
    pub(super) fn new(prefix: &str, partitions: i32) -> Result<Self, TestError> {
        Self::from_builder(
            client_builder_from_environment(&format!("kafkars-nightly-{prefix}"))?,
            prefix,
            partitions,
        )
    }

    pub(super) fn from_builder(
        builder: ClientBuilder,
        prefix: &str,
        partitions: i32,
    ) -> Result<Self, TestError> {
        let client = builder.build()?;
        ready_client(&client)?;
        let topic = unique_name(&format!("kafkars-nightly-{prefix}"));
        create_replicated_topic(&client.admin(), &topic, partitions)?;
        Ok(Self {
            client,
            topic,
            cleaned: false,
        })
    }

    pub(super) fn remove_topic(&mut self) -> Result<(), TestError> {
        if !self.cleaned {
            delete_topics(&self.client.admin(), std::slice::from_ref(&self.topic))?;
            self.cleaned = true;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<(), TestError> {
        self.remove_topic()?;
        wait_within(self.client.shutdown(), "nightly client shutdown")??;
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _cleanup = self.remove_topic();
        let _shutdown = wait_within(self.client.shutdown(), "nightly fixture cleanup shutdown");
    }
}

pub(super) fn create_replicated_topic(
    admin: &Admin,
    topic: &str,
    partitions: i32,
) -> Result<(), TestError> {
    let result = wait_within(
        admin
            .create_topics([NewTopic::new(topic, partitions).replication_factor(3)])
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "nightly CreateTopics",
    )??;
    let mut entries = result.into_entries();
    if entries.len() != 1 || entries[0].0 != topic {
        return Err(io::Error::other("CreateTopics did not return its exact topic").into());
    }
    entries.remove(0).1?;
    Ok(())
}

pub(super) fn describe_topic(admin: &Admin, topic: &str) -> Result<TopicDescription, TestError> {
    let result = wait_within(
        admin
            .describe_topics([topic])
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "nightly DescribeTopics",
    )??;
    let mut entries = result.into_entries();
    if entries.len() != 1 || entries[0].0 != topic {
        return Err(io::Error::other("DescribeTopics did not return its exact topic").into());
    }
    Ok(entries.remove(0).1?)
}

pub(super) fn poll_until<T>(mut operation: impl FnMut() -> Option<T>) -> Result<T, TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        if let Some(value) = operation() {
            return Ok(value);
        }
        if Instant::now() >= deadline {
            return Err(
                io::Error::new(io::ErrorKind::TimedOut, "nightly condition timed out").into(),
            );
        }
        thread::sleep(std::time::Duration::from_millis(50));
    }
}
