//! Reconstructible deterministic producer policy retained by the engine host.

use kafka_client_core::{
    ByteCount, CompressionPolicy, ProducerBatchPolicy, ProducerMachine, ProducerRetryPolicy,
};

#[derive(Clone, Copy, Debug)]
pub(in crate::producer) struct ProducerCoreConfig {
    pub(super) retained_bytes: ByteCount,
    pub(super) completion_capacity: usize,
    pub(super) flush_capacity: usize,
    pub(super) batch_policy: ProducerBatchPolicy,
    pub(super) retry_policy: ProducerRetryPolicy,
    pub(super) compression: CompressionPolicy,
}

impl ProducerCoreConfig {
    pub(in crate::producer) const fn machine(self) -> ProducerMachine {
        ProducerMachine::with_policies_and_flush_capacity(
            self.retained_bytes,
            self.completion_capacity,
            self.batch_policy,
            self.retry_policy,
            self.compression,
            self.flush_capacity,
        )
    }
}
