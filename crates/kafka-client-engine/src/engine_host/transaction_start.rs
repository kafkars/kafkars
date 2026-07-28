//! Rollback-safe acquisition of transaction notification and producer ownership.

use std::sync::Arc;

use crate::{
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    completion::NotifierJoin,
    consumer::{AssignedConsumerCompletionNotifier, GroupConsumerRegistry},
    driver::ReactorWake,
    producer::{ProducerHost, ProducerHostLimits},
    transaction::{
        TransactionInitializationAdmissionPort, TransactionInitializationHost,
        TransactionInitializationShardOwner,
    },
};

use super::{EngineStartError, notifier_start};

pub(super) fn start(
    limits: ProducerHostLimits,
    clock: Arc<MonotonicClock>,
    wake: &Arc<ReactorWake>,
    group_consumers: &mut GroupConsumerRegistry,
    admin: &mut AdminCompletionNotifier,
    assigned: &mut AssignedConsumerCompletionNotifier,
) -> Result<
    (
        TransactionInitializationShardOwner,
        TransactionInitializationAdmissionPort,
        ProducerHost,
    ),
    EngineStartError,
> {
    let mut transaction =
        match TransactionInitializationHost::start_with_retry_policy(limits.retry_policy) {
            Ok(host) => host,
            Err(error) => {
                rollback(None, group_consumers, admin, assigned);
                return Err(EngineStartError::transaction_notifier(&error));
            }
        };
    match ProducerHost::new_with_compression_wake(limits, wake) {
        Ok(producer) => {
            let (owner, admission) = shard(transaction, clock, Arc::clone(wake));
            Ok((owner, admission, producer))
        }
        Err(error) => {
            rollback(
                transaction.take_notifier(),
                group_consumers,
                admin,
                assigned,
            );
            Err(EngineStartError::producer(&error))
        }
    }
}

fn shard(
    host: TransactionInitializationHost,
    clock: Arc<MonotonicClock>,
    wake: Arc<ReactorWake>,
) -> (
    TransactionInitializationShardOwner,
    TransactionInitializationAdmissionPort,
) {
    let owner = TransactionInitializationShardOwner::new(host, clock, wake);
    let admission = owner.admission_port();
    (owner, admission)
}

fn rollback(
    transaction: Option<NotifierJoin>,
    group_consumers: &mut GroupConsumerRegistry,
    admin: &mut AdminCompletionNotifier,
    assigned: &mut AssignedConsumerCompletionNotifier,
) {
    notifier_start::join_acquired(transaction);
    notifier_start::join_acquired(group_consumers.take_notifier());
    notifier_start::join_acquired(admin.take_join());
    notifier_start::join_acquired(assigned.take_join());
}
