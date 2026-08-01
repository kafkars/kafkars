//! Linear tracked-call ownership for one forgotten-only broker Fetch epoch.

use kafka_client_core::Moment;
use kafka_driver::{CompletionError, RequestError, RouteFailureToken, RoutedCall};
use kafka_wire::{FetchRequest, FetchResponse as WireFetchResponse};

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::{
        consumer::remaining_timeout_ms,
        fetch::{
            FetchRequestFailure, FetchRequestSettings, FetchSessionRequest,
            OwnedForgottenFetchPartition, forgotten_fetch_request,
        },
    },
};

use super::{route::BrokerId, submission::FetchSubmitError};

#[must_use = "a forgotten-only Fetch request must be submitted or restored"]
pub(crate) struct ForgottenFetchRequest {
    settings: FetchRequestSettings,
    session: FetchSessionRequest,
    deadline: OperationDeadline,
    forgotten: Vec<OwnedForgottenFetchPartition>,
}

impl ForgottenFetchRequest {
    pub(crate) fn new(
        settings: FetchRequestSettings,
        session: FetchSessionRequest,
        deadline: OperationDeadline,
        forgotten: Vec<OwnedForgottenFetchPartition>,
    ) -> Self {
        Self {
            settings,
            session,
            deadline,
            forgotten,
        }
    }

    pub(crate) const fn session(&self) -> FetchSessionRequest {
        self.session
    }

    pub(crate) const fn deadline(&self) -> OperationDeadline {
        self.deadline
    }
}

#[must_use = "an accepted forgotten-only Fetch must settle or recover"]
pub(crate) struct TrackedForgottenFetchCall {
    request: Option<ForgottenFetchRequest>,
    call: Option<RoutedCall<WireFetchResponse>>,
}

impl TrackedForgottenFetchCall {
    #[allow(
        clippy::result_large_err,
        reason = "local rejection returns exact linear request ownership"
    )]
    pub(crate) fn submit(
        driver: &DriverOwner,
        broker_id: BrokerId,
        request: ForgottenFetchRequest,
        now: Moment,
    ) -> Result<Self, ForgottenFetchSubmitFailure> {
        let generated = match materialize(&request, now) {
            Ok(generated) => generated,
            Err(kind) => return Err(ForgottenFetchSubmitFailure { request, kind }),
        };
        let deadline = request.deadline().transport();
        let call = match driver.submit_tracked_broker_fetch(broker_id, generated, deadline) {
            Ok(call) => call,
            Err(source) => {
                return Err(ForgottenFetchSubmitFailure {
                    request,
                    kind: submit_failure_kind(source),
                });
            }
        };
        Ok(Self {
            request: Some(request),
            call: Some(call),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
        observed_at: Moment,
    ) -> Option<Result<ForgottenFetchTerminal, ForgottenFetchCompletionFailure>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        let request = self
            .request
            .take()
            .unwrap_or_else(|| panic!("tracked forgotten Fetch retains its request"));
        Some(match result {
            Err(source) => Err(ForgottenFetchCompletionFailure { request, source }),
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Ok(ForgottenFetchTerminal {
                    request,
                    observed_at,
                    selected_version: selected_version.map(kafka_driver::ApiVersion::value),
                    result,
                    confirmation: ForgottenFetchConfirmation { route_token },
                })
            }
        })
    }

    pub(crate) fn recover_after_driver_shutdown(mut self) -> ForgottenFetchRequest {
        drop(self.call.take());
        self.request
            .take()
            .unwrap_or_else(|| panic!("unsettled forgotten Fetch retains its request"))
    }
}

#[must_use = "a forgotten-only Fetch terminal must be applied or recovered"]
pub(crate) struct ForgottenFetchTerminal {
    request: ForgottenFetchRequest,
    observed_at: Moment,
    selected_version: Option<i16>,
    result: Result<WireFetchResponse, RequestError>,
    confirmation: ForgottenFetchConfirmation,
}

impl ForgottenFetchTerminal {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ForgottenFetchRequest,
        Moment,
        Option<i16>,
        Result<WireFetchResponse, RequestError>,
        ForgottenFetchConfirmation,
    ) {
        (
            self.request,
            self.observed_at,
            self.selected_version,
            self.result,
            self.confirmation,
        )
    }
}

#[must_use = "route authority must be confirmed or retained through recovery"]
pub(crate) struct ForgottenFetchConfirmation {
    route_token: Option<RouteFailureToken>,
}

impl ForgottenFetchConfirmation {
    pub(crate) fn confirm(self) {
        drop(self.route_token);
    }

    pub(crate) fn discard_after_driver_shutdown(self) {
        drop(self.route_token);
    }
}

#[must_use = "rejected forgotten-only Fetch ownership must be restored"]
pub(crate) struct ForgottenFetchSubmitFailure {
    request: ForgottenFetchRequest,
    kind: ForgottenFetchSubmitFailureKind,
}

impl ForgottenFetchSubmitFailure {
    pub(crate) fn into_parts(self) -> (ForgottenFetchRequest, ForgottenFetchSubmitFailureKind) {
        (self.request, self.kind)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForgottenFetchSubmitFailureKind {
    DeadlineElapsed,
    EmptyForgotten,
    InvalidSession,
    Request,
    Backpressured,
    DriverRejected,
}

#[must_use = "completion failure ownership is released only after driver shutdown"]
pub(crate) struct ForgottenFetchCompletionFailure {
    request: ForgottenFetchRequest,
    source: CompletionError,
}

impl ForgottenFetchCompletionFailure {
    pub(crate) fn recover_after_driver_shutdown(self) -> (ForgottenFetchRequest, CompletionError) {
        (self.request, self.source)
    }
}

pub(super) fn materialize(
    request: &ForgottenFetchRequest,
    now: Moment,
) -> Result<FetchRequest, ForgottenFetchSubmitFailureKind> {
    if request.deadline().core().is_elapsed_at(now) {
        return Err(ForgottenFetchSubmitFailureKind::DeadlineElapsed);
    }
    if request.forgotten.is_empty() {
        return Err(ForgottenFetchSubmitFailureKind::EmptyForgotten);
    }
    if !request.session().is_incremental() {
        return Err(ForgottenFetchSubmitFailureKind::InvalidSession);
    }
    let remaining = remaining_timeout_ms(now, request.deadline().core())
        .map_err(|_error| ForgottenFetchSubmitFailureKind::DeadlineElapsed)?;
    let remaining = u32::try_from(remaining)
        .map_err(|_error| ForgottenFetchSubmitFailureKind::DeadlineElapsed)?;
    forgotten_fetch_request(
        &request.forgotten,
        request.settings.cap_max_wait_ms(remaining),
        request.session(),
    )
    .map_err(|_error: FetchRequestFailure| ForgottenFetchSubmitFailureKind::Request)
}

fn submit_failure_kind(source: FetchSubmitError) -> ForgottenFetchSubmitFailureKind {
    match source {
        FetchSubmitError::Driver(kafka_driver::SubmitError::Full) => {
            ForgottenFetchSubmitFailureKind::Backpressured
        }
        FetchSubmitError::InvalidTopic(_)
        | FetchSubmitError::InvalidPartition(_)
        | FetchSubmitError::ExactBrokerRoutingUnavailable
        | FetchSubmitError::Driver(_) => ForgottenFetchSubmitFailureKind::DriverRejected,
    }
}
