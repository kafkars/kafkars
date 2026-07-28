//! Deliberately mutates close observation registration outside its owner.

struct GroupConsumerClose {
    registration: Option<u64>,
}

fn violate(close: &mut GroupConsumerClose) {
    close.registration = None;
}
