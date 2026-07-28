//! Declarative private bridge for client-quota descriptions.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeClientQuotas;
pub(crate) use request::DescribeClientQuotasAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
