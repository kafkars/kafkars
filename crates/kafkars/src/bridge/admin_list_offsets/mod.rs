//! Declarative private bridge for Admin `ListOffsets`.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminListOffsets;
pub(crate) use request::ListOffsetsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
