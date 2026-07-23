//! Virtual time and deterministic effect execution for client semantics.

#![forbid(unsafe_code)]

mod clock;

pub use clock::VirtualClock;

#[cfg(test)]
mod clock_test;
