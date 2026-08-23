//! Start, cadence, retry-gate, leave, and local-close effect interpretation.

mod retry;
#[cfg(test)]
mod retry_test;
mod start;

pub(super) use start::{consume_close_effects, map_core};
