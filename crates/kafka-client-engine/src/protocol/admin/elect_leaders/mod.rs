//! Generated API-key 43 adaptation for caller-ordered election targets.

mod model;
mod request;
mod response;
mod retention;
mod version;

pub(crate) use super::request_timeout_error::{
    AdminRequestDeadlineError as ElectLeadersDeadlineError, remaining_timeout_ms,
};
pub(crate) use model::{LeaderElectionRef, ValidatedElectLeadersResponse};
pub(crate) use request::{ElectLeadersRequestFailure, elect_leaders_request};
pub(crate) use response::{ElectLeadersProtocolFailure, validate_elect_leaders_response};
pub(crate) use retention::generated_request_peak_charge;
pub(crate) use version::{ELECT_LEADERS_MAX_VERSION, minimum_version};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod version_test;
