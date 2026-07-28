//! Inert dynamic classic-group registration policy ownership scenarios.

use std::{sync::Arc, time::Duration};

use super::GroupConsumerRegistration;

#[test]
fn request_defaults_and_processing_policy_remain_owned() {
    let request = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_processing_timeout(Duration::from_nanos(17));
    assert_eq!(request.group(), "workers");
    assert_eq!(request.topics(), &[Arc::<str>::from("orders")]);
    assert_eq!(request.processing_timeout(), Duration::from_nanos(17));

    let (group, topics, processing_policy) = request
        .into_validated_parts()
        .unwrap_or_else(|_request| panic!("positive representable timeout must validate"));
    assert_eq!(&*group, "workers");
    assert_eq!(topics, [Arc::<str>::from("orders")]);
    assert_eq!(processing_policy.timeout_ticks(), 17);
}

#[test]
fn zero_and_unrepresentable_timeouts_return_the_exact_request() {
    for timeout in [Duration::ZERO, Duration::MAX] {
        let request =
            GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
                .with_processing_timeout(timeout);
        let returned = request
            .into_validated_parts()
            .err()
            .unwrap_or_else(|| panic!("invalid processing timeout must reject"));
        assert_eq!(returned.group(), "workers");
        assert_eq!(returned.topics(), &[Arc::<str>::from("orders")]);
        assert_eq!(returned.processing_timeout(), timeout);
    }
}
