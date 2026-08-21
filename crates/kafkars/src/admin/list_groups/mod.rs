//! Public unfiltered cluster-wide group listing facade.

mod builder;
mod listing;
mod operation;
mod result;

pub use builder::ListGroupsBuilder;
pub use listing::{GroupListing, ListGroupsBrokerError};
pub use operation::ListGroups;
pub use result::ListGroupsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod listing_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
