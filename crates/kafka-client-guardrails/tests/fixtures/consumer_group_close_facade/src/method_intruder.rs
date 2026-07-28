//! Deliberately steals consumed close from a foreign file.

fn violate(owner: Consumer) {
    let _close = owner.try_close();
}
