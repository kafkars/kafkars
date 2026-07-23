//! Reactor-native engine host startup, fair execution, and terminal joining.

mod admin;
#[cfg(test)]
mod admin_test;
mod admin_wake;
mod control;
mod error;
mod lifecycle;
#[cfg(test)]
mod lifecycle_test;
mod notifier_shutdown;
#[cfg(test)]
mod notifier_shutdown_test;
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
#[cfg(test)]
mod start_test;

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
pub(crate) use start::{StartedEngineHost, start};
