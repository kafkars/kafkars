//! Fetch failures retain exact decoder context without engine-wide effects.

use kafka_wire_records::RecordError;

use super::failure::FetchDecodeFailure;

#[test]
fn record_decoder_failure_keeps_response_coordinates_and_cause() {
    let failure = FetchDecodeFailure::RecordBatch {
        topic: 2,
        partition: 3,
        batch: 4,
        source: RecordError::UnsupportedMagic { magic: 1 },
    };

    assert!(matches!(
        failure,
        FetchDecodeFailure::RecordBatch {
            topic: 2,
            partition: 3,
            batch: 4,
            source: RecordError::UnsupportedMagic { magic: 1 },
        }
    ));
}
