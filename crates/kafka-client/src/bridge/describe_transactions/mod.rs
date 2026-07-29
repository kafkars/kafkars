//! Declarative private bridge for Admin `DescribeTransactions`.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeTransactions;
pub(crate) use request::DescribeTransactionsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
