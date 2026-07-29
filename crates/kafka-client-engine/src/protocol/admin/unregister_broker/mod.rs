//! API-key 64 v0 request construction and bounded response normalization.

mod model;
mod request;
mod response;
mod retention;

pub(crate) use model::NormalizedUnregisterBrokerResponse;
#[cfg(test)]
pub(crate) use request::UnregisterBrokerRequestFailure;
pub(crate) use request::unregister_broker_request;
pub(crate) use response::{UnregisterBrokerResponseFailure, normalize_unregister_broker_response};
#[cfg(test)]
pub(crate) use retention::UNREGISTER_BROKER_MAX_RETAINED_BYTES;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
