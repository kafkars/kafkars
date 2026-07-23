//! Executable architecture policy exercised by integration tests.

#![forbid(unsafe_code)]

mod invariant_registry;

pub use invariant_registry::check_invariant_registry;
