//! Single-attempt lifecycle vocabulary for one Admin `UpdateFeatures` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    UpdateFeaturesBrokerError, UpdateFeaturesBrokerResponse, UpdateFeaturesPlan,
    UpdateFeaturesTerminal,
};

/// Current ownership stage for one finalized-feature update operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesState {
    /// Accepted but not started.
    Ready,
    /// The exact bounded plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic feature-update policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesInput {
    /// Starts the operation at one supplied monotonic observation.
    Start {
        /// Current monotonic observation supplied by the engine.
        now: Moment,
    },
    /// Reports driver ownership of the sole request.
    DriverAccepted,
    /// Reports definite rejection before driver ownership.
    DriverRejected,
    /// Reports original-deadline expiry before driver ownership.
    DeadlineElapsed,
    /// Reports original-deadline expiry after driver ownership.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports one successful version-normalized broker response.
    BrokerResponded {
        /// Older per-feature facts or version-2 atomic success.
        response: UpdateFeaturesBrokerResponse,
    },
    /// Reports one exact top-level broker rejection.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: UpdateFeaturesBrokerError,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports insufficient negotiated protocol semantics.
    ProtocolIncompatible {
        /// Authoritative certainty at incompatibility discovery.
        delivery: DeliveryStatus,
    },
    /// Reports driver-owned transport failure.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports malformed or uncorrelatable response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by feature-update policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesEffect {
    /// Submit the exact plan once through the engine's controller route.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated caller-ordered update intent.
        plan: UpdateFeaturesPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: UpdateFeaturesTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesTransition {
    effect: Option<UpdateFeaturesEffect>,
}

impl UpdateFeaturesTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: UpdateFeaturesEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<UpdateFeaturesEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved feature-update operation.
#[derive(Debug)]
pub struct UpdateFeaturesMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: UpdateFeaturesPlan,
    pub(crate) state: UpdateFeaturesState,
}

impl UpdateFeaturesMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: UpdateFeaturesPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: UpdateFeaturesState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> UpdateFeaturesState {
        self.state
    }
}

/// Rejected deterministic feature-update state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for UpdateFeaturesMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "UpdateFeatures machine rejected fact: {self:?}")
    }
}

impl std::error::Error for UpdateFeaturesMachineError {}
