//! Required pull-request qualification sequence over one mutable Kafka cluster.

use kafkars::Record;

use crate::real_broker_support::{
    TestError, TopicCleanup, client_builder_from_environment, create_topics, delete_topics,
    ready_client, unique_name, wait_within,
};

use super::{consume, evidence, nightly_support, transaction};

pub(crate) fn run_pull_request_smoke() -> Result<(), TestError> {
    let topic = unique_name("kafkars-pr-smoke");
    let group_id = unique_name("kafkars-pr-group");
    let transaction_id = unique_name("kafkars-pr-transaction");
    let client = client_builder_from_environment("kafkars-qualification-pr-smoke")?.build()?;
    let admin = client.admin();
    let producer = client.producer().build()?;

    evidence::measure("client_readiness", || ready_client(&client))?;
    evidence::measure("create_topic", || {
        create_topics(&admin, std::slice::from_ref(&topic))
    })?;
    let mut cleanup = TopicCleanup::new(admin.clone(), vec![topic.clone()]);
    let expected: [&[u8]; 2] = [b"qualification-one", b"qualification-two"];

    evidence::measure("produce", || {
        for value in expected {
            wait_within(
                producer.send(Record::to(topic.clone()).partition(0).value(value)),
                "producer delivery",
            )??;
        }
        nightly_support::flush_producer(&producer, "producer flush")?;
        Ok(())
    })?;
    evidence::measure("direct_consume", || {
        consume::direct(&client, &topic, &expected)
    })?;
    evidence::measure("classic_group_consume_commit", || {
        consume::classic_group(&client, &topic, &group_id, &expected)
    })?;
    evidence::measure("transaction_commit_abort", || {
        transaction::commit_and_abort(&client, &topic, &transaction_id)
    })?;
    evidence::measure("graceful_shutdown", || {
        nightly_support::close_producer(&producer, "producer close")?;
        delete_topics(&admin, std::slice::from_ref(&topic))?;
        cleanup.disarm();
        wait_within(client.shutdown(), "client shutdown")??;
        Ok(())
    })
}
