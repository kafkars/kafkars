//! Bounded native ownership for off-reactor `RecordBatch` compression.

mod deadline;
mod job;
mod pool;
mod wake;
mod worker;

pub(crate) use job::{CompressionCompletion, CompressionJob};
pub(crate) use pool::{
    CompressionPollError, CompressionSchedule, CompressionWorkerLimits, CompressionWorkers,
};
pub(crate) use wake::SilentCompressionWake;

#[cfg(test)]
mod pool_test;
