//! Allowed tracked Fetch mutation-owner fixture.

struct TrackedFetchCalls {
    calls: usize,
    settled: usize,
    pending_confirmation: usize,
    completion_failure: usize,
}

impl TrackedFetchCalls {
    fn advance(&mut self) {
        self.calls += 1;
        self.settled += 1;
        self.pending_confirmation += 1;
        self.completion_failure += 1;
    }
}

struct TrackedFetchCall {
    request: usize,
}

impl TrackedFetchCall {
    fn fence(&mut self) {
        self.request += 1;
    }
}

struct TrackedForgottenFetchCall {
    request: usize,
    call: usize,
}

impl TrackedForgottenFetchCall {
    fn settle(&mut self) {
        self.request += 1;
        self.call += 1;
    }
}

struct SettledFetchCall {
    terminal: usize,
}

impl SettledFetchCall {
    fn fence(&mut self) {
        self.terminal += 1;
    }
}
