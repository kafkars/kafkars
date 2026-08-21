//! Declarative private bridge for Admin `DeleteConsumerGroups`.

mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDeleteConsumerGroups;
pub(crate) use request::DeleteConsumerGroupsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
