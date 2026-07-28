//! Exhaustive core-to-engine Admin `DescribeAcls` terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeAclBinding as CoreBinding,
    DescribeAclsBrokerError as CoreBrokerError, DescribeAclsFailureKind as CoreFailureKind,
    DescribeAclsTerminal as CoreTerminal,
};

use super::{
    DescribeAclBinding, DescribeAclsBatch, DescribeAclsBrokerError, DescribeAclsDeliveryStatus,
    DescribeAclsFailure, DescribeAclsFailureKind, DescribeAclsOutcome,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeAclsOutcome {
    match terminal {
        CoreTerminal::Described(batch) => {
            let (throttle_time_ms, bindings) = batch.into_parts();
            DescribeAclsOutcome::Described(DescribeAclsBatch {
                throttle_time_ms,
                bindings: bindings.into_iter().map(translate_binding).collect(),
            })
        }
        CoreTerminal::Failed(failure) => DescribeAclsOutcome::Failed(DescribeAclsFailure {
            kind: translate_failure_kind(failure.kind().clone()),
            delivery: translate_delivery(failure.delivery()),
        }),
    }
}

fn translate_binding(binding: CoreBinding) -> DescribeAclBinding {
    let (resource_type, resource_name, pattern_type, principal, host, operation, permission_type) =
        binding.into_parts();
    DescribeAclBinding {
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
    }
}

fn translate_failure_kind(kind: CoreFailureKind) -> DescribeAclsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeAclsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeAclsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeAclsFailureKind::Transport,
        CoreFailureKind::Broker(error) => {
            DescribeAclsFailureKind::Broker(translate_broker_error(error))
        }
        CoreFailureKind::ResponseTooLarge => DescribeAclsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeAclsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeAclsFailureKind::InvalidResponse,
    }
}

fn translate_broker_error(error: CoreBrokerError) -> DescribeAclsBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    DescribeAclsBrokerError {
        code,
        message,
        message_truncated,
    }
}

const fn translate_delivery(status: CoreDeliveryStatus) -> DescribeAclsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeAclsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeAclsDeliveryStatus::PossiblySent,
    }
}
