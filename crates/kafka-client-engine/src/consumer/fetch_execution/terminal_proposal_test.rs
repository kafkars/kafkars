//! Broker-level proof classification before deterministic core application.

use kafka_client_core::Moment;

use super::{
    DirectFetchExecutor, FetchTerminalPoll,
    settlement_test::{OUTPUT_BYTES, TerminalFixture, assignment, install, prepared},
};

#[test]
fn only_partition_level_code_one_becomes_offset_reset_proof() {
    let (effect, _machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(
        &mut executor,
        prepared(effect),
        TerminalFixture::PartitionBroker(1),
    );
    let proposal = proposed(&mut executor);
    assert!(proposal.into_partition_offset_out_of_range().is_ok());

    let (effect, _machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(&mut executor, prepared(effect), TerminalFixture::Broker(1));
    let proposal = proposed(&mut executor);
    assert!(proposal.into_partition_offset_out_of_range().is_err());

    let (effect, _machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(
        &mut executor,
        prepared(effect),
        TerminalFixture::PartitionBroker(2),
    );
    let proposal = proposed(&mut executor);
    assert!(proposal.into_partition_offset_out_of_range().is_err());
}

fn proposed(executor: &mut DirectFetchExecutor) -> super::FetchTerminalProposal {
    match executor
        .poll_proposal(Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("poll terminal: {error:?}"))
    {
        FetchTerminalPoll::Proposed(proposal) => proposal,
        FetchTerminalPoll::Idle | FetchTerminalPoll::Progressed => {
            panic!("terminal proposal expected")
        }
    }
}
