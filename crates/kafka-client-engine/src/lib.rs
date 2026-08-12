//! Execution ownership between deterministic client policy and `kafka-driver`.
#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::large_stack_arrays, reason = "libtest registry"))]

mod admin;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "integrated host does not yet expose every owner metric"
    )
)]
mod clock;
mod completion;
mod config;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "direct-consumer host integration follows this concrete executor"
    )
)]
mod consumer;
mod delivery;
mod delivery_error;
mod delivery_observer;
mod driver;
mod engine;
mod engine_debug;
mod engine_host;
mod engine_transaction;
#[cfg(test)]
mod engine_transaction_test;
mod flush_error;
mod flush_observer;
mod id_hash;
mod metrics;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the first Produce slice does not yet expose every producer mechanism"
    )
)]
mod producer;
mod protocol;
mod public_api;
mod transaction;
pub use public_api::*;
#[cfg(test)]
mod config_test;
#[cfg(test)]
mod delivery_error_test;
#[cfg(test)]
mod delivery_observer_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod engine_admin_notifier_test;
#[cfg(test)]
mod engine_delete_consumer_group_offsets_test;
#[cfg(test)]
mod engine_driver_test;
#[cfg(test)]
mod engine_incremental_alter_configs_test;
#[cfg(test)]
mod engine_list_consumer_group_offsets_test;
#[cfg(test)]
mod engine_test;
#[cfg(test)]
mod flush_error_test;
#[cfg(test)]
mod flush_observer_test;
#[cfg(test)]
mod id_hash_test;
#[cfg(test)]
mod silent_broker_test;
