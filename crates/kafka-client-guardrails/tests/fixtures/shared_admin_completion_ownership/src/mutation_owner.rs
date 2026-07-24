//! Allowed shared-admin notifier lifecycle mutation fixture.

struct AdminCompletionNotifier {
    worker: usize,
}

impl AdminCompletionNotifier {
    fn stop(&mut self) {
        self.worker += 1;
    }
}
