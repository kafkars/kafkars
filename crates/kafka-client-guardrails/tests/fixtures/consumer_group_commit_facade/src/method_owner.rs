//! Deliberately modeled allowed hosted commit submission owner.

fn allowed(owner: &mut Consumer, checkpoint: Checkpoint) {
    let _accepted = owner.try_commit(checkpoint, Timeout);
}
