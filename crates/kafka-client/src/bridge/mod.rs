//! Declarative private boundary between the Rust facade and shared engine.

mod client;
pub(crate) mod producer;
pub(crate) mod producer_barrier;
pub(crate) mod producer_delivery;
pub(crate) mod producer_result;

pub(crate) use client::ClientEngine;

#[cfg(test)]
mod client_test;
#[cfg(test)]
mod producer_barrier_test;
#[cfg(test)]
mod producer_delivery_test;
#[cfg(test)]
mod producer_test;
