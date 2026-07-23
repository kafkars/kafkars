//! Tests for retained-byte capacity.

use crate::{ByteBudget, ByteCount, CapacityError};

#[test]
fn byte_budget_rejection_is_atomic() {
    let mut budget = ByteBudget::new(ByteCount::new(1_024));

    assert_eq!(budget.try_reserve(ByteCount::new(900)), Ok(()));
    assert_eq!(
        budget.try_reserve(ByteCount::new(200)),
        Err(CapacityError::Exhausted)
    );
    assert_eq!(budget.used(), ByteCount::new(900));
}
