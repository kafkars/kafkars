//! Deliberately modeled allowed event-observation owner.

fn allowed(owner: &mut Consumer) {
    let _immediate = owner.try_take_event();
    let _next = owner.next_event();
}
