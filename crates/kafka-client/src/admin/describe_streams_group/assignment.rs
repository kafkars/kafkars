//! Stable current and target `StreamsGroup` task assignments.

use super::StreamsGroupTaskIds;

/// One member's active, standby, and warm-up task assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::struct_field_names,
    reason = "the field names preserve Kafka's active, standby, and warm-up task vocabulary"
)]
pub struct StreamsGroupAssignment {
    active_tasks: Vec<StreamsGroupTaskIds>,
    standby_tasks: Vec<StreamsGroupTaskIds>,
    warmup_tasks: Vec<StreamsGroupTaskIds>,
}

impl StreamsGroupAssignment {
    pub(crate) const fn new(
        active_tasks: Vec<StreamsGroupTaskIds>,
        standby_tasks: Vec<StreamsGroupTaskIds>,
        warmup_tasks: Vec<StreamsGroupTaskIds>,
    ) -> Self {
        Self {
            active_tasks,
            standby_tasks,
            warmup_tasks,
        }
    }

    /// Returns active tasks ordered by subtopology identity.
    pub fn active_tasks(&self) -> &[StreamsGroupTaskIds] {
        &self.active_tasks
    }

    /// Returns standby tasks ordered by subtopology identity.
    pub fn standby_tasks(&self) -> &[StreamsGroupTaskIds] {
        &self.standby_tasks
    }

    /// Returns warm-up tasks ordered by subtopology identity.
    pub fn warmup_tasks(&self) -> &[StreamsGroupTaskIds] {
        &self.warmup_tasks
    }
}
