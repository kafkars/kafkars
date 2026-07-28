//! Evidence that response accounting covers all live normalized allocations.

use super::{
    AlterClientQuotasRequestRef, normalize_alter_client_quotas_response,
    request_test::{alteration, component, set},
    response_retention::{normalized_retained_charge, response_peak_charge},
};
use kafka_wire::AlterClientQuotasResponse;
use kafka_wire_core::StrBytes;

#[test]
fn response_peak_covers_request_scratch_correlation_and_projected_terminals() {
    let entity = [
        component("user", Some("User:a")),
        component("client-id", Some("orders")),
    ];
    let operations = [set("producer_byte_rate", 1024.0)];
    let alterations = [alteration(&entity, &operations)];
    let request = AlterClientQuotasRequestRef::new(&alterations, true);
    let response = response();
    let peak = response_peak_charge(request, &response).expect("bounded peak");
    let normalized = normalize_alter_client_quotas_response(1, request, &response, peak)
        .expect("peak admits full normalization");
    let retained = normalized_retained_charge(&normalized).expect("bounded normalized result");

    assert_eq!(normalized.retained_bytes, peak);
    assert!(retained < peak);
    assert!(normalize_alter_client_quotas_response(1, request, &response, peak - 1).is_err());
}

fn response() -> AlterClientQuotasResponse {
    let mut component = kafka_wire::alter_client_quotas_response::EntityData::default();
    component.entity_type = StrBytes::from("user");
    component.entity_name = Some(StrBytes::from("User:a"));
    let mut client = kafka_wire::alter_client_quotas_response::EntityData::default();
    client.entity_type = StrBytes::from("client-id");
    client.entity_name = Some(StrBytes::from("orders"));
    let mut entry = kafka_wire::alter_client_quotas_response::EntryData::default();
    entry.entity = vec![component, client];
    let mut response = AlterClientQuotasResponse::default();
    response.entries = vec![entry];
    response
}
