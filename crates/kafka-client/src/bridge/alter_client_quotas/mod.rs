//! Declarative private bridge for client-quota alterations.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminAlterClientQuotas;
pub(crate) use request::AlterClientQuotasAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
