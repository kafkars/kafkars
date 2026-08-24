//! Completed broker-local share-fetch plans retaining their assignment boundary.

use kafka_client_core::AssignmentGeneration;

use crate::clock::DeadlineCapture;

use super::super::fetch_plan::ShareBrokerSessionPlan;

/// Completed broker-local plans retaining the original assignment boundary.
#[must_use = "routed share assignment must open its broker sessions or be released"]
pub(in crate::consumer::share) struct ShareFetchRoutedAssignment {
    generation: AssignmentGeneration,
    capture: DeadlineCapture,
    plans: Vec<ShareBrokerSessionPlan>,
}

impl ShareFetchRoutedAssignment {
    pub(in crate::consumer::share) const fn new(
        generation: AssignmentGeneration,
        capture: DeadlineCapture,
        plans: Vec<ShareBrokerSessionPlan>,
    ) -> Self {
        Self {
            generation,
            capture,
            plans,
        }
    }

    pub(in crate::consumer::share) const fn generation(&self) -> AssignmentGeneration {
        self.generation
    }

    pub(in crate::consumer::share) const fn capture(&self) -> DeadlineCapture {
        self.capture
    }

    pub(in crate::consumer::share) fn into_plans(self) -> Vec<ShareBrokerSessionPlan> {
        self.plans
    }
}
