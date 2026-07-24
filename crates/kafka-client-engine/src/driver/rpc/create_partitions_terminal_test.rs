//! Semantic `CreatePartitions` terminal normalization scenarios.

use kafka_client_core::{
    CreatePartitionsInput, CreatePartitionsPlan, CreatePartitionsSpecification,
    PartitionIncreaseResult,
};
use kafka_wire::{
    CreatePartitionsResponse, create_partitions_response::CreatePartitionsTopicResult,
};

use super::create_partitions_terminal::normalize_terminal;

#[test]
fn broker_results_normalize_without_losing_exact_codes() {
    let plan = CreatePartitionsPlan::new(
        vec![CreatePartitionsSpecification::new("orders".to_owned(), 8)],
        false,
    )
    .unwrap_or_else(|error| panic!("valid partition plan: {error}"));
    let mut topic = CreatePartitionsTopicResult::default();
    topic.name = "orders".into();
    topic.error_code = -32_000;
    let mut response = CreatePartitionsResponse::default();
    response.results = vec![topic];
    let input = normalize_terminal(&plan, usize::MAX, Ok(response))
        .unwrap_or_else(|error| panic!("normalize terminal: {error:?}"));
    let CreatePartitionsInput::BrokerResponded { outcomes } = input else {
        panic!("broker response fact expected");
    };
    let (_, PartitionIncreaseResult::Failed(error)) = outcomes
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("one outcome"))
        .into_parts()
    else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -32_000);
}
