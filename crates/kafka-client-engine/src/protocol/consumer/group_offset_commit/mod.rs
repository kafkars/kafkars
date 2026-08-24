//! Classic-group `OffsetCommit` snapshots and generated-message adaptation.
//!
//! Generated DTO byte charging remains with the future group-consumer host;
//! this seam bounds only its retained scalar snapshot and normalized result.

mod entry_reservation;
mod model;
mod preparation;
mod prepared_request;
mod request;
mod response;
mod result_reservation;
mod session;
mod snapshot;
mod validation;

pub(crate) use entry_reservation::{
    GroupOffsetCommitEntryReservation, GroupOffsetCommitEntryReservationError,
};
pub(crate) use model::PreparedGroupOffsetCommit;
pub(crate) use preparation::GroupOffsetCommitPreparationError;
pub(crate) use prepared_request::{
    GroupOffsetCommitRequestPreparationError, PreparedGroupOffsetCommitRequest,
};
pub(crate) use request::group_offset_commit_request;
pub(crate) use response::{
    GroupOffsetCommitProtocolFailure, is_exact_group_offset_commit_coordinator_rejection,
    normalize_group_offset_commit_response,
};
pub(crate) use result_reservation::{
    GroupOffsetCommitResultReservation, GroupOffsetCommitResultReservationError,
};
pub(crate) use session::{ClassicGroupCommitSession, GroupOffsetCommitTopicName};

#[cfg(test)]
mod entry_reservation_test;
#[cfg(test)]
mod model_bounds_test;
#[cfg(test)]
mod model_recovery_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod prepared_request_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod result_reservation_test;
#[cfg(test)]
mod snapshot_test;
#[cfg(test)]
mod validation_test;
