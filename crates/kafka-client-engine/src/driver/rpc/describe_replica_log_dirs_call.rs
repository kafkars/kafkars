//! Linear ownership of one accepted exact-broker `DescribeReplicaLogDirs` call.

use core::mem::size_of;
use std::time::Instant;

use kafka_client_core::DescribeReplicaLogDirsReplica;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeLogDirsResponse;

use crate::protocol::admin::describe_log_dirs::{
    DescribeLogDirsSelectionRef, DescribeLogDirsTopicSelectionRef, describe_log_dirs_request,
};

use super::{
    super::DriverOwner,
    describe_replica_log_dirs_terminal::{
        DescribeReplicaLogDirsRawTerminal, RecoveredDescribeReplicaLogDirsCall,
        retain_describe_replica_log_dirs_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeReplicaLogDirs call must be terminally settled"]
pub(crate) struct DescribeReplicaLogDirsCall {
    broker_id: i32,
    call: Option<RoutedCall<DescribeLogDirsResponse>>,
}

impl DescribeReplicaLogDirsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        broker_id: i32,
        replicas: &[DescribeReplicaLogDirsReplica],
        retained_limit: usize,
        deadline: Instant,
    ) -> Result<Self, DescribeReplicaLogDirsCallAdmissionFailure> {
        let groups = selection_groups(replicas, retained_limit)?;
        let selection_bytes = groups
            .iter()
            .try_fold(0usize, |bytes, group| {
                bytes.checked_add(group.partitions.capacity().checked_mul(size_of::<i32>())?)
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    groups
                        .capacity()
                        .checked_mul(size_of::<SelectionGroup<'_>>())?,
                )
            })
            .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
        let minimum_selection_bytes = selection_bytes
            .checked_add(
                groups
                    .len()
                    .checked_mul(size_of::<DescribeLogDirsTopicSelectionRef<'_>>())
                    .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?,
            )
            .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
        retained_limit
            .checked_sub(minimum_selection_bytes)
            .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
        let mut selections = Vec::new();
        selections
            .try_reserve_exact(groups.len())
            .map_err(|_| DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
        for group in &groups {
            selections.push(DescribeLogDirsTopicSelectionRef::new(
                group.topic,
                &group.partitions,
            ));
        }
        let selection_bytes = selection_bytes
            .checked_add(
                selections
                    .capacity()
                    .checked_mul(size_of::<DescribeLogDirsTopicSelectionRef<'_>>())
                    .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?,
            )
            .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
        let request_limit = retained_limit
            .checked_sub(selection_bytes)
            .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
        let request = describe_log_dirs_request(
            DescribeLogDirsSelectionRef::Selected(&selections),
            request_limit,
        )
        .map_err(|_source| DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_describe_replica_log_dirs(broker_id, request, deadline)
            .map_err(|_source| DescribeReplicaLogDirsCallAdmissionFailure::Driver)?;
        Ok(Self {
            broker_id,
            call: Some(call),
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeReplicaLogDirsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_replica_log_dirs_terminal(
                    self.broker_id,
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredDescribeReplicaLogDirsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeReplicaLogDirsCall::new()
        })
    }
}

struct SelectionGroup<'a> {
    topic: &'a str,
    partitions: Vec<i32>,
}

fn selection_groups(
    replicas: &[DescribeReplicaLogDirsReplica],
    retained_limit: usize,
) -> Result<Vec<SelectionGroup<'_>>, DescribeReplicaLogDirsCallAdmissionFailure> {
    let worst_case_bytes = replicas
        .len()
        .checked_mul(size_of::<SelectionGroup<'_>>())
        .and_then(|bytes| bytes.checked_add(replicas.len().checked_mul(size_of::<i32>())?))
        .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
    retained_limit
        .checked_sub(worst_case_bytes)
        .ok_or(DescribeReplicaLogDirsCallAdmissionFailure::Request)?;

    let mut groups: Vec<SelectionGroup<'_>> = Vec::new();
    groups
        .try_reserve_exact(replicas.len())
        .map_err(|_| DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
    for replica in replicas {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.topic == replica.topic())
        {
            group
                .partitions
                .try_reserve(1)
                .map_err(|_| DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
            group.partitions.push(replica.partition());
        } else {
            let mut partitions = Vec::new();
            partitions
                .try_reserve_exact(1)
                .map_err(|_| DescribeReplicaLogDirsCallAdmissionFailure::Request)?;
            partitions.push(replica.partition());
            groups.push(SelectionGroup {
                topic: replica.topic(),
                partitions,
            });
        }
    }
    groups.shrink_to_fit();
    Ok(groups)
}

/// Definitely-unsent exact-route construction, request, or driver rejection.
#[must_use = "a rejected DescribeReplicaLogDirs call must become operation input"]
pub(crate) enum DescribeReplicaLogDirsCallAdmissionFailure {
    Request,
    Driver,
}
