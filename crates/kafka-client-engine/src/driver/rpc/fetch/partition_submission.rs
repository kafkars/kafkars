//! Linear submission of one prepared partition Fetch into the driver.

use kafka_client_core::Moment;
use kafka_driver::RoutedCall;
use kafka_wire::FetchResponse as WireFetchResponse;

use crate::driver::DriverOwner;

use super::{
    admission::{FetchAdmissionFailure, FetchAdmissionFailureSource, PartitionFetchRequest},
    legacy_request::generated_fetch_request,
};

pub(super) struct AcceptedFetchCall {
    pub(super) request: PartitionFetchRequest,
    pub(super) call: RoutedCall<WireFetchResponse>,
}

#[allow(
    clippy::result_large_err,
    reason = "local rejection must return the exact linear prepared Fetch without allocation"
)]
pub(super) fn submit_partition_fetch(
    driver: &DriverOwner,
    request: PartitionFetchRequest,
    now: Moment,
) -> Result<AcceptedFetchCall, FetchAdmissionFailure> {
    let (generated, partition) = match generated_fetch_request(&request, now) {
        Ok(generated) => generated,
        Err(source) => return Err(FetchAdmissionFailure::new(request, source)),
    };
    let call = match driver.submit_tracked_fetch(
        request.topic(),
        partition,
        generated,
        request.operation_deadline().transport(),
    ) {
        Ok(call) => call,
        Err(source) => {
            return Err(FetchAdmissionFailure::new(
                request,
                FetchAdmissionFailureSource::Driver(source),
            ));
        }
    };
    Ok(AcceptedFetchCall { request, call })
}
