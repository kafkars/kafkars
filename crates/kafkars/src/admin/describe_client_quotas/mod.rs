//! Declarative facade for client-quota filters, results, and observation.

mod builder;
mod entry;
mod filter;
mod operation;
mod result;
mod value;

pub use builder::DescribeClientQuotasBuilder;
pub use entry::{ClientQuotaEntityComponent, ClientQuotaEntry};
pub use filter::{ClientQuotaFilterComponent, ClientQuotaMatch};
pub use operation::DescribeClientQuotas;
pub use result::DescribeClientQuotasResult;
pub use value::ClientQuotaValue;

#[cfg(test)]
mod entry_test;
#[cfg(test)]
mod filter_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
