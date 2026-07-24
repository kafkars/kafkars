//! Deliberately mutates notifier ownership outside its lifecycle module.

struct AssignedConsumerCompletionNotifier {
    worker: Option<Worker>,
}

fn violate(owner: &mut AssignedConsumerCompletionNotifier) {
    owner.worker = None;
}
