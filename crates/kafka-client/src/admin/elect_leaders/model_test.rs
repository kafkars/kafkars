//! Stable public election-value tests.

use super::{LeaderElectionTarget, LeaderElectionType};

#[test]
fn explicit_type_and_target_are_stable_inert_values() {
    assert_eq!(LeaderElectionType::Preferred, LeaderElectionType::Preferred);
    assert_ne!(LeaderElectionType::Preferred, LeaderElectionType::Unclean);

    let target = LeaderElectionTarget::new("orders", 3);
    assert_eq!(target.topic(), "orders");
    assert_eq!(target.partition(), 3);
}
