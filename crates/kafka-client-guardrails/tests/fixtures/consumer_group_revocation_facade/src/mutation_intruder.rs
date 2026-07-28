//! Deliberately mutates revocation completion outside its owner.

struct GroupConsumerRevocationCompletion {
    completed: bool,
}

fn violate(completion: &mut GroupConsumerRevocationCompletion) {
    completion.completed = true;
}
