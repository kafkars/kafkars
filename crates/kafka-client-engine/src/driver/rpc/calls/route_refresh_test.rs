//! Exact routed-outcome authority required by Produce topic refresh.

use kafka_driver::RouteKind;

use super::route_refresh::required_route_kind;

#[test]
fn only_an_exact_broker_token_can_begin_the_topic_barrier() {
    assert_eq!(required_route_kind(), RouteKind::Broker);
}
