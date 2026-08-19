//! Private bridge for concrete leader-election alteration.

mod operation;
mod request;
mod result;

pub(crate) use operation::AdminElectLeaders;
pub(crate) use request::ElectLeadersAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
