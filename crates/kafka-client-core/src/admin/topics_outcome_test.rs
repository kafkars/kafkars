//! Constructional consistency scenarios for topic-description outcomes.

use core::num::NonZeroI16;

use super::{
    DescribeTopicBrokerError, DescribeTopicOutcome, DescribeTopicResult, TopicDescription,
};

#[test]
fn described_outcome_derives_name_and_internal_status_from_its_description() {
    let outcome = DescribeTopicOutcome::described(TopicDescription::new(
        "consumer_offsets".to_owned(),
        None,
        true,
        Vec::new(),
    ));
    assert_eq!(outcome.topic(), "consumer_offsets");
    assert!(outcome.is_internal());
    let (topic, internal, DescribeTopicResult::Described(description)) = outcome.into_parts()
    else {
        panic!("description expected");
    };
    assert_eq!(topic, description.name());
    assert_eq!(internal, description.is_internal());
}

#[test]
fn failed_outcome_retains_exact_internal_and_broker_facts() {
    let code = NonZeroI16::new(-731).unwrap_or_else(|| panic!("test code is nonzero"));
    let outcome = DescribeTopicOutcome::failed(
        "consumer_offsets",
        true,
        DescribeTopicBrokerError::new(code),
    );
    let (topic, internal, DescribeTopicResult::Failed(error)) = outcome.into_parts() else {
        panic!("error expected");
    };
    assert_eq!(topic, "consumer_offsets");
    assert!(internal);
    assert_eq!(error.code(), -731);
}
