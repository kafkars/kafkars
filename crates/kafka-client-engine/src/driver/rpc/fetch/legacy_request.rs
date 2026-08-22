//! Legacy name-routed one-partition Fetch request materialization.

use kafka_client_core::Moment;
use kafka_wire::FetchRequest;

use crate::protocol::{
    consumer::remaining_timeout_ms,
    fetch::{FetchRequestFailure, fetch_request_with_session},
};

use super::admission::{FetchAdmissionFailureSource, PartitionFetchRequest};

pub(super) fn generated_fetch_request(
    request: &PartitionFetchRequest,
    now: Moment,
) -> Result<(FetchRequest, i32), FetchAdmissionFailureSource> {
    let remaining = remaining_timeout_ms(now, request.operation_deadline().core())
        .map_err(|_error| FetchAdmissionFailureSource::DeadlineElapsed)?;
    let remaining =
        u32::try_from(remaining).map_err(|_error| FetchAdmissionFailureSource::DeadlineElapsed)?;
    let partition = request.fence().position().partition().partition().get();
    let generated = fetch_request_with_session(
        request.topic(),
        partition,
        request.next_offset().get(),
        request.settings().cap_max_wait_ms(remaining),
        request.session(),
    )
    .map_err(FetchAdmissionFailureSource::Request)?;
    let partition = i32::try_from(partition).map_err(|_error| {
        FetchAdmissionFailureSource::Request(FetchRequestFailure::PartitionOutOfRange {
            actual: partition,
        })
    })?;
    Ok((generated, partition))
}
