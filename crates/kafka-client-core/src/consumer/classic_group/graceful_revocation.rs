//! Private deterministic lease policy for delayed classic-group revocation.

mod machine;
mod model;

pub use machine::ClassicGracefulRevocation;
pub use model::{
    ClassicGracefulRevocationEffect, ClassicGracefulRevocationError,
    ClassicGracefulRevocationInput, ClassicGracefulRevocationLease,
    ClassicGracefulRevocationLossReason, ClassicGracefulRevocationTerminal,
    ClassicGracefulRevocationTransition,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
