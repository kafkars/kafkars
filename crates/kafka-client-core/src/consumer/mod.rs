//! Deterministic direct-assignment consumer ownership and fetch-position policy.

mod close;
mod effect;
mod error;
mod exports;
mod fetch_state;
mod fetch_throttle;
mod fetch_transition;
mod identity;
mod input;
mod machine;
mod model;
mod position;
mod position_ownership;
mod position_resolution;
mod position_state;
mod transition;

pub use exports::*;

#[cfg(test)]
mod assignment_test;
#[cfg(test)]
mod close_completion_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod control_test;
#[cfg(test)]
mod fetch_delivery_test;
#[cfg(test)]
mod fetch_state_test;
#[cfg(test)]
mod fetch_throttle_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod position_ownership_test;
#[cfg(test)]
mod position_state_test;
#[cfg(test)]
mod position_test;
#[cfg(test)]
mod resolution_test;
#[cfg(test)]
mod throttle_test;
#[cfg(test)]
mod transition_test;
