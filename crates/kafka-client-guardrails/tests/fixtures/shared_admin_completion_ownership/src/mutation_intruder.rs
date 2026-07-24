//! Forbidden shared-admin notifier lifecycle mutation fixture.

struct AdminCompletionNotifier {
    worker: usize,
}

impl AdminCompletionNotifier {
    fn stop_outside_owner(&mut self) {
        self.worker += 1;
    }
}
