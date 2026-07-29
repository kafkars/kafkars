//! Deterministic policy for caller-selected active-producer description.

mod failure;
mod machine;
mod model;
mod outcome;
mod transition;
mod value;

pub use failure::{AdminDescribeProducersFailure, AdminDescribeProducersFailureKind};
pub use machine::{
    AdminDescribeProducersEffect, AdminDescribeProducersInput, AdminDescribeProducersMachine,
    AdminDescribeProducersMachineError, AdminDescribeProducersState,
    AdminDescribeProducersTransition,
};
pub use model::{
    AdminDescribeProducerTarget, AdminDescribeProducersPlan, AdminDescribeProducersPlanError,
    DESCRIBE_PRODUCERS_MAX_TARGET_TOPIC_BYTES, DESCRIBE_PRODUCERS_MAX_TARGETS,
};
pub use outcome::{
    AdminDescribeProducerBrokerError, AdminDescribeProducerOutcome, AdminDescribeProducerResult,
    AdminDescribeProducersBatch, AdminDescribeProducersTerminal,
    DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES, DESCRIBE_PRODUCERS_MAX_PRODUCER_STATES,
};
pub use value::AdminProducerState;

#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
#[cfg(test)]
mod value_test;
