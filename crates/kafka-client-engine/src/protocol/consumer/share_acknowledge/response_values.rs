//! Bounded leaf-value normalization for `ShareAcknowledge` responses.

use bytes::Bytes;
use kafka_wire::share_acknowledge_response::NodeEndpoint;
use kafka_wire_core::StrBytes;

use super::{
    ShareAcknowledgeEndpoint, ShareAcknowledgeResponseFailure,
    model::SHARE_ACKNOWLEDGE_MAX_DIAGNOSTIC_BYTES,
};

pub(super) fn normalize_endpoints(
    source: Vec<NodeEndpoint>,
) -> Result<Vec<ShareAcknowledgeEndpoint>, ShareAcknowledgeResponseFailure> {
    let mut endpoints = Vec::new();
    endpoints
        .try_reserve_exact(source.len())
        .map_err(|_| ShareAcknowledgeResponseFailure::Allocation)?;
    for endpoint in source {
        if endpoint.node_id < 0 {
            return Err(ShareAcknowledgeResponseFailure::InvalidEndpointNodeId(
                endpoint.node_id,
            ));
        }
        if endpoint.host.as_str().is_empty() {
            return Err(ShareAcknowledgeResponseFailure::EmptyEndpointHost);
        }
        validate_diagnostic_size(endpoint.host.len())?;
        if let Some(rack) = &endpoint.rack {
            validate_diagnostic_size(rack.len())?;
        }
        let port = u16::try_from(endpoint.port)
            .ok()
            .filter(|port| *port != 0)
            .ok_or(ShareAcknowledgeResponseFailure::InvalidEndpointPort(
                endpoint.port,
            ))?;
        let normalized = ShareAcknowledgeEndpoint {
            node_id: endpoint.node_id,
            host: endpoint.host.into_bytes(),
            port,
            rack: endpoint.rack.map(StrBytes::into_bytes),
        };
        if endpoints
            .iter()
            .any(|candidate: &ShareAcknowledgeEndpoint| candidate.node_id == normalized.node_id)
        {
            return Err(ShareAcknowledgeResponseFailure::DuplicateEndpoint(
                normalized.node_id,
            ));
        }
        endpoints.push(normalized);
    }
    Ok(endpoints)
}

pub(super) fn normalize_leader(
    leader_id: i32,
    leader_epoch: i32,
) -> Result<Option<(i32, i32)>, ShareAcknowledgeResponseFailure> {
    match (leader_id, leader_epoch) {
        (-1, -1) => Ok(None),
        (leader, epoch) if leader >= 0 && epoch >= 0 => Ok(Some((leader, epoch))),
        _ => Err(ShareAcknowledgeResponseFailure::InvalidCurrentLeader {
            leader_id,
            leader_epoch,
        }),
    }
}

pub(super) fn diagnostic(
    value: Option<StrBytes>,
) -> Result<Option<Bytes>, ShareAcknowledgeResponseFailure> {
    let Some(value) = value else {
        return Ok(None);
    };
    validate_diagnostic_size(value.len())?;
    Ok(Some(value.into_bytes()))
}

fn validate_diagnostic_size(actual: usize) -> Result<(), ShareAcknowledgeResponseFailure> {
    if actual > SHARE_ACKNOWLEDGE_MAX_DIAGNOSTIC_BYTES {
        return Err(ShareAcknowledgeResponseFailure::DiagnosticTooLarge {
            actual,
            limit: SHARE_ACKNOWLEDGE_MAX_DIAGNOSTIC_BYTES,
        });
    }
    Ok(())
}
