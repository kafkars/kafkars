//! Fixed completion-domain worker acquisition, registration, and rollback.

use crate::{
    admin::AdminCompletionNotifier,
    completion::NotifierJoin,
    consumer::{
        AssignedConsumerCompletionNotifier, AssignedConsumerCompletionPorts, GroupConsumerRegistry,
    },
    producer::ProducerHost,
    transaction::TransactionInitializationShardOwner,
};

use super::{EngineLifecycle, EngineStartError, notifier_shutdown::NotifierShutdownOwner};

pub(super) fn start_assigned_consumer_notifier() -> Result<
    (
        AssignedConsumerCompletionNotifier,
        AssignedConsumerCompletionPorts,
    ),
    EngineStartError,
> {
    AssignedConsumerCompletionNotifier::start()
        .map_err(|error| EngineStartError::assigned_consumer_notifier(&error))
}

pub(super) fn install_thread_ids(
    lifecycle: &EngineLifecycle,
    producer: &ProducerHost,
    admin: &AdminCompletionNotifier,
    assigned_consumer: &AssignedConsumerCompletionNotifier,
    group_consumers: &GroupConsumerRegistry,
    transaction_initialization: &TransactionInitializationShardOwner,
) {
    for thread_id in [
        producer.notifier_thread_id(),
        admin.thread_id(),
        assigned_consumer.thread_id(),
        group_consumers.notifier_thread_id(),
        transaction_initialization.notifier_thread_id(),
    ]
    .into_iter()
    .flatten()
    {
        lifecycle.install_notifier_thread(thread_id);
    }
}

pub(super) fn join_acquired(notifier: Option<NotifierJoin>) {
    let Some(notifier) = notifier else {
        return;
    };
    let mut owner = NotifierShutdownOwner::new(vec![notifier]);
    let _join_result = owner.join_off_notifier();
}
