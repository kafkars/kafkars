//! Deliberately modeled allowed consumed-close owner.

fn allowed(owner: Consumer) {
    let _close = owner.try_close();
}
