//! Public Admin API for listing configuration-resource identities.

mod builder;
mod operation;
mod resource;
mod result;

pub use builder::ListConfigResourcesBuilder;
pub use operation::ListConfigResources;
pub use resource::{ConfigResource, ConfigResourceType};
pub use result::ListConfigResourcesResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod resource_test;
#[cfg(test)]
mod result_test;
