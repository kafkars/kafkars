//! Private exact classic-group checkpoint translation API-shape contract.

use kafka_client_engine::GroupConsumerCheckpoint as EngineCheckpoint;

use super::group_consumer_checkpoint::GroupConsumerCheckpoint;

#[test]
fn bridge_preserves_exact_linear_engine_checkpoint() {
    fn checkpoint_contract(checkpoint: &GroupConsumerCheckpoint) {
        let _: &str = checkpoint.topic();
        let _: i32 = checkpoint.partition();
        let _: i64 = checkpoint.next_offset();
    }
    fn round_trip(checkpoint: EngineCheckpoint) -> EngineCheckpoint {
        GroupConsumerCheckpoint::from_engine(checkpoint).into_engine()
    }

    let _ = checkpoint_contract as fn(&GroupConsumerCheckpoint);
    let _ = round_trip as fn(EngineCheckpoint) -> EngineCheckpoint;
}
