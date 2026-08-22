//! Broker-aggregated name-based Fetch request construction and session control.

use std::sync::Arc;

use kafka_wire::{
    FetchRequest,
    fetch_request::{FetchTopic, ForgottenTopic},
};
use kafka_wire_core::Uuid;

use super::{
    request::{
        FetchRequestFailure, FetchRequestSettings, base_request, generated_partition,
        validate_topic,
    },
    session::FetchSessionRequest,
};

/// One active partition included in an exact broker-owned Fetch request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BrokerFetchPartition<'a> {
    topic: &'a str,
    topic_id: [u8; 16],
    leader_epoch: Option<i32>,
    partition: u32,
    fetch_offset: i64,
}

impl<'a> BrokerFetchPartition<'a> {
    pub(crate) const fn new(
        topic: &'a str,
        topic_id: [u8; 16],
        leader_epoch: Option<i32>,
        partition: u32,
        fetch_offset: i64,
    ) -> Self {
        Self {
            topic,
            topic_id,
            leader_epoch,
            partition,
            fetch_offset,
        }
    }
}

/// One partition explicitly removed from a live broker-owned Fetch session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ForgottenFetchPartition<'a> {
    topic: &'a str,
    topic_id: [u8; 16],
    partition: u32,
}

impl<'a> ForgottenFetchPartition<'a> {
    pub(crate) const fn new(topic: &'a str, topic_id: [u8; 16], partition: u32) -> Self {
        Self {
            topic,
            topic_id,
            partition,
        }
    }
}

/// One owned partition identity retained by a forgotten-only Fetch call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OwnedForgottenFetchPartition {
    topic: Arc<str>,
    topic_id: [u8; 16],
    partition: u32,
}

impl OwnedForgottenFetchPartition {
    pub(crate) fn new(topic: Arc<str>, topic_id: [u8; 16], partition: u32) -> Self {
        Self {
            topic,
            topic_id,
            partition,
        }
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) const fn partition(&self) -> u32 {
        self.partition
    }

    pub(crate) const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }
}

/// Builds one broker-routed request containing every active and forgotten partition.
pub(crate) fn broker_fetch_request(
    active: &[BrokerFetchPartition<'_>],
    forgotten: &[ForgottenFetchPartition<'_>],
    settings: FetchRequestSettings,
    session: FetchSessionRequest,
) -> Result<FetchRequest, FetchRequestFailure> {
    let mut request = base_request(settings, session)?;
    request
        .topics
        .try_reserve_exact(active.len())
        .map_err(|_error| FetchRequestFailure::Allocation)?;
    for item in active {
        validate_topic(item.topic)?;
        let mut partition = generated_partition(item.partition, item.fetch_offset, settings)?;
        partition.current_leader_epoch = item.leader_epoch.unwrap_or(-1);
        let topic = find_or_insert_topic(&mut request.topics, item.topic, item.topic_id)?;
        topic
            .partitions
            .try_reserve(1)
            .map_err(|_error| FetchRequestFailure::Allocation)?;
        topic.partitions.push(partition);
    }
    append_forgotten(
        &mut request,
        forgotten.len(),
        forgotten
            .iter()
            .map(|item| (item.topic, item.topic_id, item.partition)),
    )?;
    Ok(request)
}

/// Builds one incremental request whose only session delta removes partitions.
pub(crate) fn forgotten_fetch_request(
    forgotten: &[OwnedForgottenFetchPartition],
    settings: FetchRequestSettings,
    session: FetchSessionRequest,
) -> Result<FetchRequest, FetchRequestFailure> {
    let mut request = base_request(settings, session)?;
    append_forgotten(
        &mut request,
        forgotten.len(),
        forgotten
            .iter()
            .map(|item| (item.topic(), item.topic_id(), item.partition())),
    )?;
    Ok(request)
}

/// Builds Kafka's final-epoch request for one established broker session.
pub(crate) fn fetch_session_close_request(
    settings: FetchRequestSettings,
    session: FetchSessionRequest,
) -> Option<Result<FetchRequest, FetchRequestFailure>> {
    let close = if session.is_close() {
        Some(session)
    } else {
        session.close()
    };
    close.map(|close| base_request(settings, close))
}

fn find_or_insert_topic<'a>(
    topics: &'a mut Vec<FetchTopic>,
    name: &str,
    topic_id: [u8; 16],
) -> Result<&'a mut FetchTopic, FetchRequestFailure> {
    if let Some(index) = topics
        .iter()
        .position(|topic| topic.topic_id.to_bytes() == topic_id)
    {
        return Ok(&mut topics[index]);
    }
    let mut topic = FetchTopic::default();
    topic.topic = name.into();
    topic.topic_id = Uuid::from_bytes(topic_id);
    topics.push(topic);
    let index = topics.len().saturating_sub(1);
    Ok(&mut topics[index])
}

fn find_or_insert_forgotten<'a>(
    topics: &'a mut Vec<ForgottenTopic>,
    name: &str,
    topic_id: [u8; 16],
) -> Result<&'a mut ForgottenTopic, FetchRequestFailure> {
    if let Some(index) = topics
        .iter()
        .position(|topic| topic.topic_id.to_bytes() == topic_id)
    {
        return Ok(&mut topics[index]);
    }
    let mut topic = ForgottenTopic::default();
    topic.topic = name.into();
    topic.topic_id = Uuid::from_bytes(topic_id);
    topics.push(topic);
    let index = topics.len().saturating_sub(1);
    Ok(&mut topics[index])
}

fn append_forgotten<'a>(
    request: &mut FetchRequest,
    count: usize,
    forgotten: impl Iterator<Item = (&'a str, [u8; 16], u32)>,
) -> Result<(), FetchRequestFailure> {
    request
        .forgotten_topics_data
        .try_reserve_exact(count)
        .map_err(|_error| FetchRequestFailure::Allocation)?;
    for (name, topic_id, partition) in forgotten {
        validate_topic(name)?;
        let partition = i32::try_from(partition)
            .map_err(|_error| FetchRequestFailure::PartitionOutOfRange { actual: partition })?;
        let topic = find_or_insert_forgotten(&mut request.forgotten_topics_data, name, topic_id)?;
        topic
            .partitions
            .try_reserve(1)
            .map_err(|_error| FetchRequestFailure::Allocation)?;
        topic.partitions.push(partition);
    }
    Ok(())
}
