//! End-to-end prepared-byte, submission-deadline, and release scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionId, BatchId, ByteCount, CompressionPolicy, Deadline, DeliveryStatus, Moment,
    OperationId, PartitionIndex, ProducerCompletion, ProducerEffect, ProducerFailureKind,
    ProducerIdentityGeneration, ProducerInput, ProducerMachine,
};

use super::{
    ProducerRecord, ProducerStore, ProducerStoreLimits,
    binding::OperationBindings,
    execution::{PreparedExecution, PreparedExecutionLimits},
};
use crate::{clock::OperationDeadline, completion::CompletionId};

struct SealedBatch {
    core: ProducerMachine,
    store: ProducerStore,
    bindings: OperationBindings,
    operation_id: OperationId,
    execution_id: BatchExecutionId,
}

impl SealedBatch {
    fn new(deadline: Deadline) -> Self {
        let record = ProducerRecord::new(
            Arc::from("orders"),
            PartitionIndex::from_raw(7),
            100,
            Some(Bytes::from_static(b"key")),
            Some(Bytes::from_static(b"value")),
        );
        let mut store = ProducerStore::new(ProducerStoreLimits {
            records: 1,
            bytes: 1_024,
            batches: 1,
        });
        let reservation = store
            .reserve(record)
            .unwrap_or_else(|error| panic!("record reservation failed: {error}"));
        let facts = reservation.facts();
        let mut core = ProducerMachine::new(ByteCount::new(1_024), 1);
        let admitted = core
            .apply(ProducerInput::AdmitExplicit {
                now: Moment::from_tick(0),
                deadline,
                record: facts,
            })
            .unwrap_or_else(|error| panic!("core admission failed: {error}"));
        let (operation_id, batch_id) = accumulation(admitted.effects());
        store
            .commit(reservation)
            .unwrap_or_else(|error| panic!("record commit failed: {error}"));
        let accumulated_bytes = store
            .accumulate(batch_id, operation_id, facts.payload_id())
            .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
        let waiting = core
            .apply(ProducerInput::RecordAccumulated {
                operation_id,
                batch_id,
                accumulator_bytes: accumulated_bytes,
                now: Moment::from_tick(0),
            })
            .unwrap_or_else(|error| panic!("core accumulation failed: {error}"));
        let generation = waiting
            .effects()
            .iter()
            .find_map(|effect| match effect {
                ProducerEffect::AcquireProducerIdentity { generation, .. } => Some(*generation),
                _ => None,
            })
            .unwrap_or_else(ProducerIdentityGeneration::initial);
        let sealed = core
            .apply(ProducerInput::ProducerIdentityAcquired {
                generation,
                producer_id: 1,
                producer_epoch: 0,
                now: Moment::from_tick(0),
            })
            .unwrap_or_else(|error| panic!("test identity failed: {error}"));
        let execution_id = sealed
            .effects()
            .iter()
            .find_map(|effect| match effect {
                ProducerEffect::MaterializeBatch {
                    execution,
                    compression: CompressionPolicy::Uncompressed,
                    ..
                } if execution.batch_id() == batch_id => Some(*execution),
                _ => None,
            })
            .unwrap_or_else(|| panic!("sealed batch must request materialization"));
        let mut bindings = OperationBindings::new(1);
        bindings
            .bind(
                operation_id,
                CompletionId::from_parts_for_test(0, 1),
                OperationDeadline::from_parts_for_test(deadline, Instant::now()),
            )
            .unwrap_or_else(|error| panic!("operation binding failed: {error}"));
        Self {
            core,
            store,
            bindings,
            operation_id,
            execution_id,
        }
    }

    fn materialize(&mut self, execution: &mut PreparedExecution, now: Moment) -> ProducerInput {
        execution
            .materialize(
                &mut self.store,
                self.execution_id,
                CompressionPolicy::Uncompressed,
                now,
            )
            .unwrap_or_else(|error| panic!("engine materialization failed: {error}"))
    }

    fn apply_and_arm(&mut self, execution: &mut PreparedExecution, input: ProducerInput) {
        let transition = self
            .core
            .apply(input)
            .unwrap_or_else(|error| panic!("materialized fact failed: {error}"));
        let [effect @ ProducerEffect::SubmitProduce { .. }] = transition.effects() else {
            panic!("materialized batch must request one submission")
        };
        execution
            .arm_submission(&self.store, &self.bindings, *effect)
            .unwrap_or_else(|error| panic!("submission arm failed: {error}"));
    }
}

#[test]
fn materialization_retains_one_bounded_request_without_fake_driver_acceptance() {
    let mut batch = SealedBatch::new(Deadline::from_tick(10));
    let mut execution = execution(1_024);
    let fact = batch.materialize(&mut execution, Moment::from_tick(1));

    assert_eq!(
        fact,
        ProducerInput::BatchMaterialized {
            execution: batch.execution_id,
            now: Moment::from_tick(1),
        }
    );
    assert_eq!(execution.prepared_stats().batches, 1);
    assert!(execution.prepared_stats().encoded_record_bytes > 0);
    assert_eq!(execution.submission_count(), 0);

    batch.apply_and_arm(&mut execution, fact);
    assert_eq!(execution.prepared_stats().batches, 1);
    assert_eq!(execution.submission_count(), 1);
    assert_eq!(execution.next_deadline(), Some(Deadline::from_tick(10)));
    assert!(execution.drain_due(Moment::from_tick(9), 1).is_empty());
}

#[test]
fn pre_driver_expiry_releases_encoded_and_original_bytes_as_not_sent() {
    let mut batch = SealedBatch::new(Deadline::from_tick(10));
    let mut execution = execution(1_024);
    let materialized = batch.materialize(&mut execution, Moment::from_tick(1));
    batch.apply_and_arm(&mut execution, materialized);

    let due = execution.drain_due(Moment::from_tick(10), 1);
    assert_eq!(
        due,
        [ProducerInput::DeadlineElapsed {
            operation_id: batch.operation_id,
            now: Moment::from_tick(10),
        }]
    );
    let terminal = batch
        .core
        .apply(due[0])
        .unwrap_or_else(|error| panic!("deadline fact failed: {error}"));
    let completion = interpret_terminal(&mut batch, &mut execution, terminal.into_effects());

    let ProducerCompletion::Failed(failure) = completion else {
        panic!("pre-driver deadline must fail")
    };
    assert_eq!(failure.kind(), ProducerFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert_eq!(execution.prepared_stats().batches, 0);
    assert_eq!(execution.prepared_stats().encoded_record_bytes, 0);
    assert_eq!(execution.submission_count(), 0);
    assert_eq!(batch.store.stats().records, 0);
    assert_eq!(batch.store.stats().bytes, 0);
    assert_eq!(batch.store.stats().batches, 0);
}

#[test]
fn encoded_capacity_failure_becomes_a_core_owned_not_sent_outcome() {
    let mut batch = SealedBatch::new(Deadline::from_tick(10));
    let mut execution = execution(0);
    let failed = batch.materialize(&mut execution, Moment::from_tick(1));
    assert_eq!(
        failed,
        ProducerInput::BatchMaterializationFailed {
            execution: batch.execution_id,
        }
    );
    assert_eq!(execution.prepared_stats().batches, 0);
    assert_eq!(execution.submission_count(), 0);

    let terminal = batch
        .core
        .apply(failed)
        .unwrap_or_else(|error| panic!("failure fact failed: {error}"));
    let completion = interpret_terminal(&mut batch, &mut execution, terminal.into_effects());
    let ProducerCompletion::Failed(failure) = completion else {
        panic!("materialization rejection must fail")
    };
    assert_eq!(failure.kind(), ProducerFailureKind::MaterializationFailed);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert_eq!(batch.store.stats().records, 0);
}

fn accumulation(effects: &[ProducerEffect]) -> (OperationId, BatchId) {
    effects
        .iter()
        .find_map(|effect| match effect {
            ProducerEffect::AccumulateExplicit {
                operation_id,
                batch_id,
                ..
            } => Some((*operation_id, *batch_id)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("admission must emit accumulation"))
}

fn interpret_terminal(
    batch: &mut SealedBatch,
    execution: &mut PreparedExecution,
    effects: Vec<ProducerEffect>,
) -> ProducerCompletion {
    let mut completion = None;
    for effect in effects {
        match effect {
            ProducerEffect::ReleaseBatch { batch_id } => execution
                .release_batch(&mut batch.store, batch_id)
                .unwrap_or_else(|error| panic!("batch release failed: {error}")),
            ProducerEffect::ReleasePayload {
                payload_id,
                retained_bytes,
            } => batch
                .store
                .release_payload(payload_id, retained_bytes)
                .unwrap_or_else(|error| panic!("payload release failed: {error}")),
            ProducerEffect::Complete {
                operation_id,
                completion: terminal,
            } if operation_id == batch.operation_id => completion = Some(terminal),
            other => panic!("unexpected terminal effect: {other:?}"),
        }
    }
    completion.unwrap_or_else(|| panic!("terminal completion missing"))
}

const fn execution(encoded_bytes: usize) -> PreparedExecution {
    PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes,
            max_batch_bytes: 1_024,
        },
    )
}
