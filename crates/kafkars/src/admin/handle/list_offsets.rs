//! Caller-ordered offset-query entry point on the shared admin handle.

use super::Admin;
use crate::{
    admin::{ListOffsetsBuilder, ListOffsetsQuery},
    bridge::admin_list_offsets::ListOffsetsAdminRequest,
};

impl Admin {
    /// Builds an inert caller-ordered offset query.
    pub fn list_offsets<I>(&self, queries: I) -> ListOffsetsBuilder
    where
        I: IntoIterator<Item = ListOffsetsQuery>,
    {
        let request = ListOffsetsAdminRequest::new(queries.into_iter().collect());
        ListOffsetsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }
}
