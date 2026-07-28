//! Deterministic policy for one read-only Admin `DescribeClientQuotas` query.

mod machine;
mod model;
mod outcome;
mod transition;
mod value;

pub use machine::{
    DescribeClientQuotasEffect, DescribeClientQuotasInput, DescribeClientQuotasMachine,
    DescribeClientQuotasMachineError, DescribeClientQuotasState, DescribeClientQuotasTransition,
};
pub use model::{
    ClientQuotaMatch, DescribeClientQuotaFilterComponent, DescribeClientQuotasPlan,
    DescribeClientQuotasPlanError,
};
pub use outcome::{
    DESCRIBE_CLIENT_QUOTAS_DIAGNOSTIC_BYTES, DescribeClientQuotasBatch,
    DescribeClientQuotasBrokerError, DescribeClientQuotasFailure, DescribeClientQuotasFailureKind,
    DescribeClientQuotasTerminal,
};
pub use value::{
    DescribeClientQuotaEntity, DescribeClientQuotaEntityComponent, DescribeClientQuotaValue,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
