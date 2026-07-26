//! Next-event port close and owner-loss observation scenarios.

use std::sync::Arc;

use crate::consumer::assigned_host::{claim::AssignedConsumerClaimSlot, shard_test::setup};

#[test]
fn closed_empty_event_fifo_is_end_of_stream() {
    let (owner, port, _reactor_wake) = setup();
    let (slot, closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    closer
        .close()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));

    assert!(matches!(handle.next_event().wait(), Ok(None)));
    assert!(
        owner
            .try_with_owner(|assigned| assigned.unsettled())
            .is_ok()
    );
}
