//! Deliberately steals hosted commit admission from a foreign file.

fn violate(owner: &mut Consumer, checkpoint: Checkpoint) {
    let _accepted = owner.try_commit(checkpoint, Timeout);
}
