//! Engine-owned scalar intent for partition-reassignment listing.

use kafka_client_core::{
    ListPartitionReassignmentTarget as CoreTarget, ListPartitionReassignmentsPlan,
    ListPartitionReassignmentsPlanError,
};

/// One inert caller-ordered topic-partition selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPartitionReassignmentTarget {
    topic: String,
    partition: i32,
}

impl ListPartitionReassignmentTarget {
    /// Creates one target for validation at the admission boundary.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }
}

/// Explicit inert request mode; selected and all-active cannot be confused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListPartitionReassignmentsRequestSelection {
    /// One caller-ordered batch validated during submission.
    Selected(Vec<ListPartitionReassignmentTarget>),
    /// Every active reassignment visible through the controller.
    AllActive,
}

/// One inert controller reassignment query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPartitionReassignmentsRequest {
    selection: ListPartitionReassignmentsRequestSelection,
}

impl ListPartitionReassignmentsRequest {
    /// Creates one inert explicit topic-partition selection.
    pub const fn selected(targets: Vec<ListPartitionReassignmentTarget>) -> Self {
        Self {
            selection: ListPartitionReassignmentsRequestSelection::Selected(targets),
        }
    }

    /// Creates one explicit all-active query.
    pub const fn all_active() -> Self {
        Self {
            selection: ListPartitionReassignmentsRequestSelection::AllActive,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        if let ListPartitionReassignmentsRequestSelection::Selected(targets) = &mut self.selection {
            for target in targets {
                target.topic = target.topic.clone().into_boxed_str().into_string();
            }
        }
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<ListPartitionReassignmentsPlan, ListPartitionReassignmentsPlanError> {
        match self.selection {
            ListPartitionReassignmentsRequestSelection::Selected(targets) => {
                ListPartitionReassignmentsPlan::selected(
                    targets
                        .into_iter()
                        .map(|target| CoreTarget::new(target.topic, target.partition))
                        .collect(),
                )
            }
            ListPartitionReassignmentsRequestSelection::AllActive => {
                Ok(ListPartitionReassignmentsPlan::all_active())
            }
        }
    }
}
