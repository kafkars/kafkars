//! The exact driver RPC subtree may own tracked generated calls.

use kafka_driver::RoutedCall;
use kafka_wire::ProduceResponse;

fn retain(_: RoutedCall<ProduceResponse>) {}
