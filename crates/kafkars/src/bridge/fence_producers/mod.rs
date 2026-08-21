//! Declarative private bridge for caller-ordered transactional producer fencing.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminFenceProducers;
pub(crate) use request::FenceProducersAdminRequest;

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
