//! Declarative bridge facade for producer submission and observation.

mod barrier;
mod delivery;
mod handle;

pub(crate) use barrier::ProducerBarrier;
pub(crate) use delivery::ProducerDelivery;
pub(crate) use handle::{ProducerEngine, restore_rejected_record};

#[cfg(test)]
mod barrier_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod handle_test;
