//! Transactional producer-fencing entry point on the shared public admin handle.

use std::time::Instant;

use super::Admin;
use crate::{admin::FenceProducersBuilder, bridge::fence_producers::FenceProducersAdminRequest};

impl Admin {
    /// Builds an inert caller-ordered batch that fences transactional producers.
    pub fn fence_producers<I, S>(&self, transactional_ids: I) -> FenceProducersBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let boundary = Instant::now();
        self.fence_producers_from_boundary(transactional_ids, boundary)
    }

    pub(super) fn fence_producers_from_boundary<I, S>(
        &self,
        transactional_ids: I,
        boundary: Instant,
    ) -> FenceProducersBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let request = FenceProducersAdminRequest::new(
            transactional_ids.into_iter().map(Into::into).collect(),
        );
        FenceProducersBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
            boundary,
        )
    }
}
