//! Scenario-shaped loopback broker state and metadata fixtures.

mod wire;

use kafka_wire::{
    CreateTopicsRequest, CreateTopicsResponse, MetadataRequest, MetadataResponse,
    create_topics_response::CreatableTopicResult,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
};
use kafka_wire_core::{StrBytes, Uuid};

use crate::driver::DriverOwner;

use self::wire::{LoopbackConnection, LoopbackListener};

pub(super) struct LoopbackBroker {
    listener: LoopbackListener,
    seed: Option<LoopbackConnection>,
    controller: Option<LoopbackConnection>,
}

impl LoopbackBroker {
    pub(super) fn bind() -> Self {
        Self {
            listener: LoopbackListener::bind(),
            seed: None,
            controller: None,
        }
    }

    pub(super) fn address(&self) -> String {
        self.listener.address()
    }

    pub(super) fn initialize(&mut self, driver: &mut DriverOwner) {
        LoopbackListener::await_seed(driver);
        let mut seed = self.listener.accept(driver);
        seed.negotiate(driver);
        let port = self.listener.port();
        seed.respond::<MetadataRequest, _>(
            driver,
            &cluster_metadata(port),
            "install controller metadata",
        );
        self.seed = Some(seed);
    }

    pub(super) fn respond_create_topics(&mut self, driver: &mut DriverOwner) {
        let mut controller = self.listener.accept(driver);
        controller.negotiate(driver);
        controller.respond::<CreateTopicsRequest, _>(
            driver,
            &create_topics_response(),
            "settle CreateTopics",
        );
        assert!(self.controller.is_none());
        self.controller = Some(controller);
    }

    pub(super) fn respond_unknown_topic(&mut self, driver: &mut DriverOwner) {
        let port = self.listener.port();
        self.seed_mut().respond::<MetadataRequest, _>(
            driver,
            &unknown_topic_metadata(port),
            "install lagging topic metadata",
        );
    }

    pub(super) fn respond_visible_topic(&mut self, driver: &mut DriverOwner) {
        let port = self.listener.port();
        self.seed_mut().respond::<MetadataRequest, _>(
            driver,
            &visible_topic_metadata(port),
            "install visible topic metadata",
        );
    }

    pub(super) fn assert_no_frame_before_retry(&self) {
        self.seed
            .as_ref()
            .unwrap_or_else(|| panic!("seed connection must be initialized"))
            .assert_no_frame();
    }

    fn seed_mut(&mut self) -> &mut LoopbackConnection {
        self.seed
            .as_mut()
            .unwrap_or_else(|| panic!("seed connection must be initialized"))
    }
}

fn create_topics_response() -> CreateTopicsResponse {
    let mut fresh = CreatableTopicResult::default();
    fresh.name = StrBytes::from("fresh");
    fresh.num_partitions = 2;
    fresh.replication_factor = 1;
    let mut existing = CreatableTopicResult::default();
    existing.name = StrBytes::from("existing");
    existing.error_code = 36;
    let mut response = CreateTopicsResponse::default();
    response.topics = vec![fresh, existing];
    response
}

fn cluster_metadata(port: u16) -> MetadataResponse {
    let mut response = MetadataResponse::default();
    response.brokers.push(metadata_broker(port));
    response.controller_id = 1;
    response
}

fn unknown_topic_metadata(port: u16) -> MetadataResponse {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(StrBytes::from("fresh"));
    topic.error_code = 3;
    let mut response = cluster_metadata(port);
    response.topics.push(topic);
    response
}

fn visible_topic_metadata(port: u16) -> MetadataResponse {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(StrBytes::from("fresh"));
    topic.topic_id = Uuid::from_bytes([7; 16]);
    for partition_index in 0..2 {
        let mut partition = MetadataResponsePartition::default();
        partition.partition_index = partition_index;
        partition.leader_id = 1;
        topic.partitions.push(partition);
    }
    let mut response = cluster_metadata(port);
    response.topics.push(topic);
    response
}

fn metadata_broker(port: u16) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(port);
    broker
}
