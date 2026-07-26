//! Assigned classic-group API-key 9 request preparation and normalization.

mod model;
mod preparation;
mod request;
mod response;
mod response_validation;
mod response_view;
mod retention;

pub(crate) use model::{
    GroupOffsetFetchCorrelation, GroupOffsetFetchPartitionValueRef, GroupOffsetFetchTopic,
    NormalizedGroupOffsetFetch,
};
pub(crate) use preparation::{
    GroupOffsetFetchPreparation, GroupOffsetFetchRequestPreparationFailure,
    PreparedGroupOffsetFetch, PreparedGroupOffsetFetchRequest, prepare_group_offset_fetch_request,
};
pub(crate) use request::GroupOffsetFetchRequest;
pub(crate) use response::{GroupOffsetFetchProtocolFailure, normalize_group_offset_fetch_response};

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod preparation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod response_validation_test;
#[cfg(test)]
mod response_view_test;
#[cfg(test)]
mod retention_test;
