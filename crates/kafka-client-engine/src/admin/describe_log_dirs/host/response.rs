//! Exhaustive raw-driver and protocol-terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminDescribeLogDirsBrokerError, AdminDescribeLogDirsBrokerOutcome, AdminDescribeLogDirsInput,
    AdminDescribeLogDirsSelection, AdminLogDirDescription, AdminLogDirOutcome,
    AdminLogDirReplicaInfo, DeliveryStatus,
};

use crate::{
    driver::{
        DescribeLogDirsDriverFailureKind, DescribeLogDirsRawTerminal, DescribeLogDirsTerminalFact,
    },
    protocol::admin::describe_log_dirs::{
        DescribeLogDirsResponseFailure, DescribeLogDirsSelectionResponseFailure,
        NormalizedDescribeLogDir, NormalizedDescribeLogDirsResponse,
        normalize_describe_log_dirs_response_for_selection,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeLogDirsRawTerminal,
    selection: &AdminDescribeLogDirsSelection,
) -> (AdminDescribeLogDirsInput, usize) {
    match raw.fact() {
        DescribeLogDirsTerminalFact::Response {
            broker_id,
            selected_version: Some(selected_version),
            response,
        } => {
            match normalize_describe_log_dirs_response_for_selection(
                selection,
                selected_version,
                response,
                raw.result_limit(),
            ) {
                Ok(normalized) => normalized_input(broker_id, normalized)
                    .unwrap_or((AdminDescribeLogDirsInput::ResponseTooLarge, 0)),
                Err(
                    DescribeLogDirsSelectionResponseFailure::RetainedBytes
                    | DescribeLogDirsSelectionResponseFailure::Response(
                        DescribeLogDirsResponseFailure::RetainedBytes { .. },
                    ),
                ) => (AdminDescribeLogDirsInput::ResponseTooLarge, 0),
                Err(DescribeLogDirsSelectionResponseFailure::Response(
                    DescribeLogDirsResponseFailure::UnsupportedApiVersion { .. },
                )) => (
                    AdminDescribeLogDirsInput::ProtocolIncompatible {
                        delivery: DeliveryStatus::PossiblySent,
                    },
                    0,
                ),
                Err(_) => (AdminDescribeLogDirsInput::InvalidResponse, 0),
            }
        }
        DescribeLogDirsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            AdminDescribeLogDirsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeLogDirsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    broker_id: i32,
    normalized: NormalizedDescribeLogDirsResponse,
) -> Result<(AdminDescribeLogDirsInput, usize), ()> {
    let (throttle_time_ms, error_code, log_dirs, retained_bytes) = normalized.into_parts();
    let outcome = if let Some(code) = NonZeroI16::new(error_code) {
        AdminDescribeLogDirsBrokerOutcome::broker_failed(
            broker_id,
            AdminDescribeLogDirsBrokerError::new(code),
        )
    } else {
        let mut outcomes = Vec::new();
        outcomes.try_reserve_exact(log_dirs.len()).map_err(|_| ())?;
        for log_dir in log_dirs {
            outcomes.push(normalize_log_dir(log_dir)?);
        }
        AdminDescribeLogDirsBrokerOutcome::described(broker_id, outcomes)
    };
    Ok((
        AdminDescribeLogDirsInput::BrokerResponded {
            throttle_time_ms,
            outcome,
        },
        retained_bytes,
    ))
}

fn normalize_log_dir(normalized: NormalizedDescribeLogDir) -> Result<AdminLogDirOutcome, ()> {
    let (error_code, path, topics, total_bytes, usable_bytes, cordoned) = normalized.into_parts();
    if let Some(code) = NonZeroI16::new(error_code) {
        Ok(AdminLogDirOutcome::broker_failed(
            path,
            AdminDescribeLogDirsBrokerError::new(code),
        ))
    } else {
        let replica_count = topics.iter().try_fold(0usize, |count, topic| {
            count.checked_add(topic.partitions().len())
        });
        let mut replicas = Vec::new();
        replicas
            .try_reserve_exact(replica_count.ok_or(())?)
            .map_err(|_| ())?;
        for topic in topics {
            let (name, partitions) = topic.into_parts();
            for partition in partitions {
                let (partition, size_bytes, offset_lag, future) = partition.into_parts();
                replicas.push(AdminLogDirReplicaInfo::new(
                    copy_string(&name)?,
                    partition,
                    size_bytes,
                    offset_lag,
                    future,
                ));
            }
        }
        Ok(AdminLogDirOutcome::described(
            path,
            AdminLogDirDescription::new(replicas, total_bytes, usable_bytes, cordoned),
        ))
    }
}

fn copy_string(source: &str) -> Result<String, ()> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_| ())?;
    owned.push_str(source);
    Ok(owned)
}

const fn driver_failure(
    kind: DescribeLogDirsDriverFailureKind,
    delivery: DeliveryStatus,
) -> AdminDescribeLogDirsInput {
    match kind {
        DescribeLogDirsDriverFailureKind::DeadlineElapsed => {
            AdminDescribeLogDirsInput::DriverDeadlineElapsed { delivery }
        }
        DescribeLogDirsDriverFailureKind::Compatibility => {
            AdminDescribeLogDirsInput::ProtocolIncompatible { delivery }
        }
        DescribeLogDirsDriverFailureKind::InvalidResponse => {
            AdminDescribeLogDirsInput::InvalidResponse
        }
        DescribeLogDirsDriverFailureKind::Transport => {
            AdminDescribeLogDirsInput::TransportFailed { delivery }
        }
    }
}
