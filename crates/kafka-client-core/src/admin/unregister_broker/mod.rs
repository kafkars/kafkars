//! Deterministic policy for one destructive Admin `UnregisterBroker` request.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    UnregisterBrokerEffect, UnregisterBrokerInput, UnregisterBrokerMachine,
    UnregisterBrokerMachineError, UnregisterBrokerState, UnregisterBrokerTransition,
};
pub use model::{UnregisterBrokerPlan, UnregisterBrokerPlanError};
pub use outcome::{
    UNREGISTER_BROKER_DIAGNOSTIC_BYTES, UnregisterBrokerBrokerError, UnregisterBrokerFailure,
    UnregisterBrokerFailureKind, UnregisterBrokerSuccess, UnregisterBrokerTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
