//! Generated API-key 30 adaptation for one caller-ordered ACL creation batch.

mod model;
mod request;
mod response;
mod retention;
mod version;

pub(crate) use model::{CreateAclBindingRef, NormalizedCreateAclResultRef};
#[cfg(test)]
pub(crate) use request::CreateAclsRequestFailure;
pub(crate) use request::create_acls_request;
pub(crate) use response::{CreateAclsResponseFailure, normalize_create_acls_response};
pub(crate) use version::{CREATE_ACLS_MAX_VERSION, CREATE_ACLS_MIN_VERSION};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod version_test;
