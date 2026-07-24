//! Receive-port close observation scenarios.

use std::sync::Arc;

use crate::consumer::assigned_host::{claim::AssignedConsumerClaimSlot, shard_test::setup};

#[test]
fn closed_admission_is_end_of_stream_without_fetch_work() {
    let (_owner, port, _reactor_wake) = setup();
    let (slot, closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    closer
        .close()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));
    let terminal = handle
        .recv()
        .wait()
        .unwrap_or_else(|error| panic!("observe closed receive: {error}"));
    assert!(terminal.is_none());
}
