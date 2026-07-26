//! Synthetic raw decoding and foreign policy capabilities.

use kafka_driver::RoutedCall;
use kafka_wire::ControlRecordTypeSchema;
use kafka_wire_core::{Decoder, KafkaDecode};
use std::time::Instant;

struct Retry;

async fn decode(batch: &Batch) {
    let _call: Option<RoutedCall<()>> = None;
    let _schema = ControlRecordTypeSchema::default();
    let _decoder = Decoder::new(bytes(), limits());
    let _trait_name = core::any::type_name::<dyn KafkaDecode>();
    let _now = Instant::now();
    let _retry = Retry;
    let _result = decode_control_record(batch);
}
