//! Reactor-native engine host startup, fair execution, and terminal joining.

mod control;
mod error;
mod recovery;
#[cfg(test)]
mod recovery_test;
mod runner;
#[cfg(test)]
mod runner_test;
mod start;

pub(crate) use control::EngineHostControl;
#[cfg(test)]
pub(crate) use control::EngineHostSnapshot;
pub(crate) use error::EngineHostError;
pub use error::{EngineShutdownError, EngineStartError, EngineStartErrorKind};
pub(crate) use recovery::recover;
pub(crate) use runner::{EngineHostExit, EngineHostResources, run};
pub(crate) use start::{EngineHostJoin, StartedEngineHost, start};
