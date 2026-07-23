//! Machine-readable semantic invariant registry checks.

mod evidence;
mod model;
mod validation;

#[cfg(test)]
#[path = "evidence_test.rs"]
mod evidence_test;
#[cfg(test)]
#[path = "model_test.rs"]
mod model_test;
#[cfg(test)]
#[path = "validation_test.rs"]
mod validation_test;

pub use validation::check_invariant_registry;
