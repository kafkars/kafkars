//! Public synchronous processing-acknowledgment shape contract.

use super::{Checkpoint, Consumer, ConsumerAcknowledgeError};

#[test]
fn acknowledgment_consumes_one_checkpoint_without_an_operation_observer() {
    fn require(
        _acknowledge: fn(&mut Consumer, Checkpoint) -> Result<(), ConsumerAcknowledgeError>,
    ) {
    }

    require(Consumer::acknowledge);
}
