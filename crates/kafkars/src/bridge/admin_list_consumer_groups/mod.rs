//! Declarative private bridge for cluster-wide consumer-group listing.

mod operation;
mod result;

pub(crate) use operation::AdminListConsumerGroups;
