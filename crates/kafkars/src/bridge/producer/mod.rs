//! Declarative bridge facade for producer submission and observation.

mod barrier;
mod batch;
mod conversion;
mod delivery;
mod handle;
mod send;

pub(crate) use barrier::ProducerBarrier;
pub(crate) use batch::ProducerBatch;
pub(crate) use conversion::{PreparedEngineRecords, prepare_engine_record, prepare_engine_records};
pub(crate) use delivery::ProducerDelivery;
pub(crate) use handle::ProducerEngine;
pub(crate) use send::ProducerSend;

#[cfg(test)]
mod barrier_test;
#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod conversion_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod handle_test;
