//! Declarative private bridge for destructive legacy topic configuration replacement.

mod engine;
mod operation;
mod request;
mod resource_operation;
mod result;

pub(crate) use operation::AdminLegacyReplaceTopicConfigs;
pub(crate) use request::LegacyReplaceTopicConfigsAdminRequest;
pub(crate) use resource_operation::AdminLegacyReplaceConfigResources;

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod resource_operation_test;
#[cfg(test)]
mod result_test;
