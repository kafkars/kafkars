//! Declarative private bridge for finalized-feature updates.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminUpdateFeatures;
pub(crate) use request::translate_request;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
