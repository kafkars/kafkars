//! Deliberately steals event observation from a foreign file.

fn violate(owner: &mut Consumer) {
    let _immediate = owner.try_take_event();
    let _next = owner.next_event();
}
