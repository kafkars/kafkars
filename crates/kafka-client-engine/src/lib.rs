//! Execution ownership between deterministic client policy and `kafka-driver`.

#![forbid(unsafe_code)]

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
mod delivery;
mod delivery_error;
mod delivery_observer;
mod driver;
mod engine;
mod engine_host;
mod flush_error;
mod flush_observer;
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
mod engine_driver_test;
#[cfg(test)]
mod engine_test;
#[cfg(test)]
mod flush_error_test;
#[cfg(test)]
mod flush_observer_test;
#[cfg(test)]
mod silent_broker_test;
