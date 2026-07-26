//! Reactor-native engine host startup, fair execution, and terminal joining.

mod admin;
#[cfg(test)]
mod admin_test;
mod admin_wake;
mod admission_close;
mod assigned_consumer;
mod assigned_consumer_start;
#[cfg(test)]
mod assigned_consumer_start_test;
#[cfg(test)]
mod assigned_consumer_test;
mod assigned_consumer_wake;
#[cfg(test)]
mod assigned_consumer_wake_test;
mod cleanup;
#[cfg(test)]
mod cleanup_test;
mod control;
mod describe_configs_start;
mod error;
mod finalize;
#[cfg(test)]
mod finalize_test;
mod group_consumer;
mod group_consumer_shutdown;
#[cfg(test)]
mod group_consumer_shutdown_test;
#[cfg(test)]
mod group_consumer_test;
mod group_consumer_wake;
#[cfg(test)]
mod group_consumer_wake_test;
mod lifecycle;
#[cfg(test)]
mod lifecycle_test;
mod notifier_shutdown;
#[cfg(test)]
mod notifier_shutdown_test;
mod notifier_start;
#[cfg(test)]
mod notifier_start_test;
mod produce;
#[cfg(test)]
mod produce_test;
mod produce_turn;
#[cfg(test)]
mod produce_turn_test;
mod recovery;
#[cfg(test)]
mod recovery_test;
mod runner;
#[cfg(test)]
mod runner_test;
mod start;
mod start_handoff;
#[cfg(test)]
mod start_handoff_test;

pub(crate) use control::EngineHostControl;
#[cfg(test)]
pub(crate) use control::EngineHostSnapshot;
pub(crate) use error::EngineHostError;
pub use error::{
    EngineShutdownError, EngineShutdownErrorKind, EngineStartError, EngineStartErrorKind,
};
pub(crate) use lifecycle::EngineLifecycle;
pub(crate) use recovery::recover;
pub(crate) use runner::{EngineHostExit, EngineHostResources, run};
pub(crate) use start::start;
pub(crate) use start_handoff::StartedEngineHost;
