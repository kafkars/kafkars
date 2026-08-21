//! Declarative private bridge for client-metrics resource listings.

mod engine;
mod operation;
mod result;

pub(crate) use operation::AdminListClientMetricsResources;

#[cfg(test)]
mod result_test;
