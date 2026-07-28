//! Unobserved initialization success queues owner-loss cleanup.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::sync_channel,
};

use kafka_client_core::TransactionalOwnerId;

use super::RetainedTransactionInitializationOutcome;

#[test]
fn unobserved_payload_queues_idle_owner_loss_without_releasing_execution() {
    let (sender, receiver) = sync_channel(1);
    let active = Arc::new(AtomicBool::new(true));
    let retained = RetainedTransactionInitializationOutcome::initialized(
        TransactionalOwnerId::from_raw(7),
        Arc::<str>::from("writer"),
        41,
        3,
        Arc::clone(&active),
        sender,
        std::time::Duration::from_secs(45),
    );

    drop(retained);

    assert!(active.load(Ordering::Acquire));
    let signal = receiver
        .try_recv()
        .unwrap_or_else(|error| panic!("idle owner-loss signal: {error:?}"));
    assert_eq!(signal.owner_id.get(), 7);
    assert_eq!(signal.deadline, None);
}
