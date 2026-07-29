//! Public explicit leader-election model, builder, operation, and result.

mod builder;
mod model;
mod operation;
mod result;

pub use builder::ElectLeadersBuilder;
pub use model::{LeaderElectionTarget, LeaderElectionType};
pub use operation::ElectLeaders;
pub use result::ElectLeadersResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod operation_test;
