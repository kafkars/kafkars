//! Generated API-key 31 adaptation for caller-ordered ACL deletion filters.

mod model;
mod request;
mod response;
mod response_validation;
mod response_value;
mod retention;
mod version;

pub(crate) use model::DeleteAclsFilterRef;
#[cfg(test)]
pub(crate) use model::NormalizedDeleteAclsResponse;
#[cfg(test)]
pub(crate) use request::DeleteAclsRequestFailure;
pub(crate) use request::delete_acls_request;
pub(crate) use response::{DeleteAclsResponseFailure, normalize_delete_acls_response};
pub(crate) use version::{DELETE_ACLS_MAX_VERSION, DELETE_ACLS_MIN_VERSION};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod version_test;
