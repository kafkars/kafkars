//! Inert group-offset deletion ownership and rejection scenarios.

use std::time::Duration;

use crate::{
    Client, DeleteConsumerGroupOffsetsBuilder, DeliveryStatus, ErrorKind, StartPosition,
    TopicPartition,
};

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<DeleteConsumerGroupOffsetsBuilder>();
}

#[test]
fn any_assignment_start_position_is_rejected_definitely_unsent_at_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    for start in [
        StartPosition::Beginning,
        StartPosition::End,
        StartPosition::Offset(9),
    ] {
        let error = client
            .admin()
            .delete_consumer_group_offsets(
                "payments",
                [
                    TopicPartition::new("orders", 0),
                    TopicPartition::new("audit", 1).start_at(start),
                ],
            )
            .deadline_after(Duration::from_secs(1))
            .submit()
            .wait()
            .err()
            .unwrap_or_else(|| panic!("assignment start position must reject at submit"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}

#[test]
fn zero_deadline_is_deferred_to_the_submit_boundary() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .delete_consumer_group_offsets("payments", [TopicPartition::new("orders", 0)])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
