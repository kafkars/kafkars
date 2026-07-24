//! Concrete automatic-assignment `CreatePartitions` ownership domain.
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;
pub use error::{CreatePartitionsAdmissionError, CreatePartitionsAdmissionErrorKind};
pub use handle::{CreatePartitionsAccepted, CreatePartitionsAcceptedFaultKind};
pub(crate) use host::{
    CREATE_PARTITIONS_CAPACITY, CreatePartitionsHost, CreatePartitionsHostError,
    CreatePartitionsTurn,
};
pub use model::{CreatePartitionsRequest, PartitionIncrease};
pub use observer::CreatePartitionsObserver;
pub use outcome::{
    CreatePartitionsDeliveryStatus, CreatePartitionsFailure, CreatePartitionsFailureKind,
    CreatePartitionsObserverError, CreatePartitionsOutcome, PartitionIncreaseError,
    PartitionIncreaseResult,
};
pub(crate) use shard::{
    CreatePartitionsAdmissionPort, CreatePartitionsShardLockError, CreatePartitionsShardOwner,
    CreatePartitionsShardWake, CreatePartitionsShardWakeError,
};
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod shard_test;
