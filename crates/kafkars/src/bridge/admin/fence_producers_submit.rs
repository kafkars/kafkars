//! Sole engine-submission seam for producer-fencing requests.

use std::time::Instant;

use super::AdminEngine;
use crate::bridge::fence_producers::{AdminFenceProducers, FenceProducersAdminRequest};

impl AdminEngine {
    pub(crate) fn submit_fence_producers(
        &self,
        request: FenceProducersAdminRequest,
        deadline: Instant,
    ) -> AdminFenceProducers {
        AdminFenceProducers::submit_with(request, deadline, |request, remaining| {
            self.handle.try_fence_producers(request, remaining)
        })
    }
}
