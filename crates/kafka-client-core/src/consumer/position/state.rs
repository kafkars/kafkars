//! Retained pause and fetch-position state for one assigned partition.

use crate::consumer::{
    AssignedTopicPartition, AssignmentEpoch, fetch_state::RetainedFetchActivation,
    position_state::PartitionPosition,
};

use super::RetainedResolutionActivation;

#[derive(Debug)]
pub(in crate::consumer) struct AssignedPartitionState {
    pub(in crate::consumer) assignment_epoch: AssignmentEpoch,
    pub(in crate::consumer) partition: AssignedTopicPartition,
    pub(super) paused: bool,
    pub(super) position: PartitionPosition,
}

pub(in crate::consumer) enum RetainedResumePlan {
    AlreadyResumed,
    ResumeFetch(Option<RetainedFetchActivation>),
    ResumeResolution(RetainedResolutionActivation),
}
