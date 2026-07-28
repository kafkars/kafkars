//! Declarative facade for deterministic Admin `DeleteRecords` policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    DeleteRecordsEffect, DeleteRecordsInput, DeleteRecordsMachine, DeleteRecordsMachineError,
    DeleteRecordsState, DeleteRecordsTransition,
};
pub use model::{DeleteRecordsPlan, DeleteRecordsPlanError, DeleteRecordsTarget};
pub use outcome::{
    DeleteRecordsBatch, DeleteRecordsBrokerError, DeleteRecordsFailure, DeleteRecordsFailureKind,
    DeleteRecordsOutcome, DeleteRecordsResult, DeleteRecordsTerminal, DeletedRecords,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
