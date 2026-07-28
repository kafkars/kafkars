//! Declarative private bridge for Admin `DeleteRecords`.

mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDeleteRecords;
pub(crate) use request::DeleteRecordsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
