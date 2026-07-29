//! Selected and cluster-wide leader-election bridge translation tests.

use kafka_client_engine::{
    ElectLeadersRequest as EngineRequest, LeaderElectionTarget as EngineTarget,
    LeaderElectionType as EngineType,
};

use crate::{LeaderElectionTarget, LeaderElectionType};

use super::ElectLeadersAdminRequest;

#[test]
fn selected_translation_preserves_type_target_order_and_empty_selection() {
    let selected = ElectLeadersAdminRequest::new(
        LeaderElectionType::Unclean,
        vec![
            LeaderElectionTarget::new("zeta", 3),
            LeaderElectionTarget::new("audit", 1),
        ],
    );
    assert_eq!(
        selected.into_engine(),
        EngineRequest::selected(
            EngineType::Unclean,
            vec![EngineTarget::new("zeta", 3), EngineTarget::new("audit", 1),],
        )
    );

    let empty = ElectLeadersAdminRequest::new(LeaderElectionType::Preferred, Vec::new());
    assert_eq!(
        empty.into_engine(),
        EngineRequest::selected(EngineType::Preferred, Vec::new())
    );
}

#[test]
fn all_translation_is_explicit_and_carries_no_selected_targets() {
    let request = ElectLeadersAdminRequest::all(LeaderElectionType::Preferred);

    assert_eq!(
        request.into_engine(),
        EngineRequest::all(EngineType::Preferred)
    );
}
