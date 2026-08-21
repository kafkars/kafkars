//! Public Admin API for listing client-metrics configuration resources.

mod builder;
mod operation;
mod result;

pub use builder::ListClientMetricsResourcesBuilder;
pub use operation::ListClientMetricsResources;
pub use result::ListClientMetricsResourcesResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
