//! Generated-free terminal facts from one bounded voter-addition response.

/// One validated API-key 80 terminal with exact signed broker status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAddRaftVoterResponse {
    throttle_time_ms: u32,
    broker_error_code: i16,
    diagnostic: Option<String>,
    diagnostic_truncated: bool,
    retained_bytes: usize,
}

impl NormalizedAddRaftVoterResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        broker_error_code: i16,
        diagnostic: Option<String>,
        diagnostic_truncated: bool,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            broker_error_code,
            diagnostic,
            diagnostic_truncated,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, i16, Option<String>, bool, usize) {
        (
            self.throttle_time_ms,
            self.broker_error_code,
            self.diagnostic,
            self.diagnostic_truncated,
            self.retained_bytes,
        )
    }
}
