//! Curated direct-assignment policy exports.

pub use super::effect::{
    AssignedConsumerEffect, AssignedConsumerTransition, FetchFailure, FetchThrottleFailure,
    PositionResolutionFailure,
};
pub use super::error::AssignedConsumerMachineError;
pub use super::input::AssignedConsumerInput;
pub use super::machine::AssignedConsumerMachine;
pub use super::model::{
    AssignedPartition, AssignedTopicPartition, AssignmentEpoch, FetchFence, FetchOwnership,
    FetchRecords, FetchRevision, NextFetchOffset, PositionEpoch, PositionFence, PositionOwnership,
    StartPosition,
};
