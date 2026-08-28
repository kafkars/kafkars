//! Immediate exact-broker transfer from a reserved lane into driver ownership.

use kafka_client_core::Moment;

use crate::{
    driver::{DriverOwner, ProduceCallPermit},
    producer::{execution::PreparedProduceSubmission, ingress::ProducerShardData},
};

use super::super::super::{EngineHostError, reject_execution};

pub(super) fn submit_group(
    driver: &DriverOwner,
    permit: ProduceCallPermit<'_>,
    data: &mut ProducerShardData,
    mut submissions: Vec<PreparedProduceSubmission>,
    now: Moment,
    accepted_in_flight_requests: usize,
    accepted_broker_in_flight_requests: usize,
) -> Result<(), EngineHostError> {
    let request_batches = submissions.len();
    let request_records = submissions.iter().fold(0_u64, |total, submission| {
        total.saturating_add(u64::from(submission.record_count()))
    });
    let request_bytes = submissions.iter().fold(0_usize, |total, submission| {
        total.saturating_add(submission.encoded_record_bytes())
    });
    if submissions.len() == 1 {
        let submission = submissions
            .pop()
            .unwrap_or_else(|| unreachable!("singleton routed Produce group is nonempty"));
        let (execution, deadline, materialized) = submission.into_parts();
        match permit.submit(driver, execution, deadline, materialized, now) {
            Ok(accepted) => {
                record_request(
                    data,
                    request_batches,
                    request_records,
                    request_bytes,
                    accepted_in_flight_requests,
                    accepted_broker_in_flight_requests,
                );
                data.apply_produce_driver_input(now, accepted.driver_accepted())
                    .map_err(EngineHostError::Producer)?;
                accepted.confirm_receipt();
            }
            Err(rejection) => {
                let failure = rejection.failure_kind();
                drop(rejection);
                reject_execution(data, execution, now, failure)?;
            }
        }
    } else {
        match permit.submit_batch(driver, submissions, now) {
            Ok(accepted) => {
                record_request(
                    data,
                    request_batches,
                    request_records,
                    request_bytes,
                    accepted_in_flight_requests,
                    accepted_broker_in_flight_requests,
                );
                for input in accepted.inputs() {
                    data.apply_produce_driver_input(now, input)
                        .map_err(EngineHostError::Producer)?;
                }
                accepted.confirm_receipt();
            }
            Err(rejection) => {
                let failure = rejection.failure_kind();
                for execution in rejection.executions() {
                    reject_execution(data, execution, now, failure)?;
                }
            }
        }
    }
    Ok(())
}

fn record_request(
    data: &mut ProducerShardData,
    batches: usize,
    records: u64,
    bytes: usize,
    in_flight_requests: usize,
    broker_in_flight_requests: usize,
) {
    data.record_produce_request(
        batches,
        records,
        bytes,
        in_flight_requests,
        broker_in_flight_requests,
    );
}
