//! Declarative assembly of concrete direct-consumer Fetch execution.

#[path = "fetch_execution/admission.rs"]
mod admission;
#[cfg(test)]
mod admission_test;
#[path = "fetch_execution/apply.rs"]
mod apply;
#[path = "fetch_execution/control.rs"]
mod control;
#[cfg(test)]
mod control_test;
#[path = "fetch_execution/deadline.rs"]
mod deadline;
#[cfg(test)]
mod deadline_test;
#[path = "fetch_execution/delivery.rs"]
mod delivery;
#[path = "fetch_execution/executor.rs"]
mod executor;
#[path = "fetch_execution/fault.rs"]
mod fault;
#[cfg(test)]
mod fault_test;
#[path = "fetch_execution/prepared.rs"]
mod prepared;
#[cfg(test)]
mod prepared_test;
#[path = "fetch_execution/settlement.rs"]
mod settlement;
#[cfg(test)]
mod settlement_test;
#[cfg(test)]
mod stale_test;
#[path = "fetch_execution/terminal.rs"]
mod terminal;

#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "the direct-consumer host will consume this concrete executor facade"
    )
)]
pub(crate) use admission::FetchSubmission;
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "the direct-consumer owner will capture each internal Fetch attempt boundary"
    )
)]
pub(crate) use deadline::FetchAttemptDeadline;
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "the direct-consumer host will consume this concrete executor facade"
    )
)]
pub(crate) use executor::DirectFetchExecutor;
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "the direct-consumer host will consume this concrete executor facade"
    )
)]
pub(crate) use fault::FetchExecutionError;
pub(crate) use fault::FetchReclaimFailure;
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "the direct-consumer host will consume this concrete executor facade"
    )
)]
pub(crate) use prepared::{PrepareFetchError, PreparedFetchExecution};
#[cfg(test)]
pub(super) use settlement_test::{
    TerminalFixture as FetchTerminalFixture, install as install_terminal_for_test,
};
