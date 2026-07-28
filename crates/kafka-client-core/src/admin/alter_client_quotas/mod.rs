//! Deterministic policy for one destructive Admin `AlterClientQuotas` batch.

mod entity;
mod machine;
mod model;
mod operation;
mod outcome;
mod transition;

pub use entity::{AlterClientQuotaEntity, AlterClientQuotaEntityComponent};
pub use machine::{
    AlterClientQuotasEffect, AlterClientQuotasInput, AlterClientQuotasMachine,
    AlterClientQuotasMachineError, AlterClientQuotasState, AlterClientQuotasTransition,
};
pub use model::{
    ALTER_CLIENT_QUOTAS_MAX_COMPONENTS_PER_ENTITY, ALTER_CLIENT_QUOTAS_MAX_ENTRIES,
    ALTER_CLIENT_QUOTAS_MAX_OPERATIONS_PER_ENTITY, ALTER_CLIENT_QUOTAS_MAX_STRING_BYTES,
    AlterClientQuotaEntry, AlterClientQuotasPlan, AlterClientQuotasPlanError,
};
pub use operation::{AlterClientQuotaOperation, AlterClientQuotaOperationKind};
pub use outcome::{
    ALTER_CLIENT_QUOTAS_DIAGNOSTIC_BYTES, AlterClientQuotaBrokerError, AlterClientQuotaOutcome,
    AlterClientQuotaResult, AlterClientQuotasBatch, AlterClientQuotasFailure,
    AlterClientQuotasFailureKind, AlterClientQuotasTerminal,
};

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
