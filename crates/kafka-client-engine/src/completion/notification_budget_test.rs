//! Checked notification-capacity arithmetic and joint construction scenarios.

use super::{NotificationBudget, NotificationBudgetError};

#[test]
fn notification_budget_rejects_overflow_before_owner_construction() {
    let error = NotificationBudget::try_new(usize::MAX, 1, usize::MAX)
        .err()
        .unwrap_or_else(|| panic!("N + P overflow must be rejected"));
    assert_eq!(error, NotificationBudgetError::CapacityOverflow);
}

#[test]
fn notification_budget_rejects_declared_total_mismatch() {
    let error = NotificationBudget::try_new(2, 3, 4)
        .err()
        .unwrap_or_else(|| panic!("declared total must equal N + P"));
    assert_eq!(error, NotificationBudgetError::TotalMismatch);
}

#[test]
fn notification_budget_constructs_both_exact_owners() {
    let budget = NotificationBudget::try_new(2, 3, 5)
        .unwrap_or_else(|error| panic!("budget should validate: {error:?}"));
    let owners = budget
        .start::<u8>()
        .unwrap_or_else(|error| panic!("notifier should start: {error}"));
    let (mut completions, permits) = owners.into_parts();
    assert_eq!(permits.capacity(), 3);
    let join = completions
        .stop_notifier()
        .unwrap_or_else(|error| panic!("empty notifier should stop: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
