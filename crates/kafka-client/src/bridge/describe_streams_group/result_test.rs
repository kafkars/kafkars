//! StreamsGroup description translation and exact value-loopback tests.

use crate::{DeliveryStatus, ErrorKind, admin::StreamsGroupTopologyDescriptionStatus};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, Assignment, DeliveryStatus as EngineDeliveryStatus,
        Description, Endpoint, FailureKind, KeyValue, Member, ObserverError, Outcome, Subtopology,
        TaskIds, TaskOffset, TopicInfo, Topology, TopologyDescription,
        TopologyDescriptionGlobalStore, TopologyDescriptionNode, TopologyDescriptionStatus,
        TopologyDescriptionSubtopology,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_failure_parts,
        translate_observation, translate_observer_error,
    },
};

#[test]
fn admission_categories_are_exhaustive_and_definitely_unsent() {
    for (kind, expected) in [
        (AdmissionErrorKind::InvalidRequest, ErrorKind::Configuration),
        (
            AdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (AdmissionErrorKind::Contended, ErrorKind::Backpressure),
        (AdmissionErrorKind::Capacity, ErrorKind::Backpressure),
        (AdmissionErrorKind::RetainedBytes, ErrorKind::Backpressure),
        (AdmissionErrorKind::Closed, ErrorKind::State),
        (AdmissionErrorKind::IdentityExhausted, ErrorKind::Internal),
        (AdmissionErrorKind::HostUnavailable, ErrorKind::Internal),
    ] {
        let error = translate_admission_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}

#[test]
fn mechanism_failures_preserve_category_and_exact_delivery() {
    for (kind, expected) in [
        (FailureKind::DeadlineElapsed, ErrorKind::Timeout),
        (FailureKind::DriverRejected, ErrorKind::Backpressure),
        (FailureKind::Transport, ErrorKind::Transport),
        (FailureKind::ResponseTooLarge, ErrorKind::Backpressure),
        (FailureKind::Compatibility, ErrorKind::Compatibility),
        (FailureKind::InvalidResponse, ErrorKind::Broker),
    ] {
        for (delivery, expected_delivery) in [
            (EngineDeliveryStatus::NotSent, DeliveryStatus::NotSent),
            (
                EngineDeliveryStatus::PossiblySent,
                DeliveryStatus::PossiblySent,
            ),
        ] {
            let error = translate_failure_parts(kind, delivery);
            assert_eq!(error.kind(), expected);
            assert_eq!(error.delivery_status(), Some(expected_delivery));
        }
    }
}

#[test]
fn accepted_and_observer_failures_keep_stable_categories() {
    for fault in [AcceptedFaultKind::Wake, AcceptedFaultKind::HostInvariant] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
    assert_eq!(
        translate_observer_error(ObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(ObserverError::Stale).kind(),
        ErrorKind::Internal
    );
}

#[test]
fn exact_nested_engine_result_loops_back_without_losing_nullable_or_future_facts() {
    let source = TopologyDescriptionNode::new(
        "source".to_owned(),
        1,
        vec!["orders".to_owned()],
        None,
        Vec::new(),
        vec!["processor".to_owned()],
    );
    let processor = TopologyDescriptionNode::new(
        "processor".to_owned(),
        91,
        Vec::new(),
        Some("orders-out".to_owned()),
        vec!["counts".to_owned()],
        Vec::new(),
    );
    let description = Description::new(
        "streams-workers".to_owned(),
        "Stable".to_owned(),
        11,
        13,
        Some(Topology::new(
            7,
            Some(vec![Subtopology::new(
                "sub-a".to_owned(),
                vec!["orders".to_owned()],
                vec!["orders-repartition".to_owned()],
                Vec::new(),
                vec![TopicInfo::new(
                    "orders-repartition".to_owned(),
                    6,
                    3,
                    vec![KeyValue::new(
                        "cleanup.policy".to_owned(),
                        "delete".to_owned(),
                    )],
                )],
            )]),
        )),
        vec![Member::new(
            "member-a".to_owned(),
            9,
            Some("instance-a".to_owned()),
            Some("rack-a".to_owned()),
            "client-a".to_owned(),
            "/127.0.0.1".to_owned(),
            7,
            "process-a".to_owned(),
            Some(Endpoint::new("localhost".to_owned(), 8080)),
            vec![KeyValue::new("rack".to_owned(), "az-a".to_owned())],
            vec![TaskOffset::new("sub-a".to_owned(), 0, 91)],
            vec![TaskOffset::new("sub-a".to_owned(), 0, 100)],
            Assignment::new(
                vec![TaskIds::new("sub-a".to_owned(), vec![0, 2])],
                Vec::new(),
                Vec::new(),
            ),
            Assignment::new(
                Vec::new(),
                vec![TaskIds::new("sub-a".to_owned(), vec![1])],
                Vec::new(),
            ),
            true,
        )],
        Some(0x21),
        Some(TopologyDescription::new(
            vec![TopologyDescriptionSubtopology::new(
                "sub-a".to_owned(),
                vec![processor.clone()],
            )],
            vec![TopologyDescriptionGlobalStore::new(source, processor)],
        )),
        Some(TopologyDescriptionStatus::new(89)),
    );
    let result = super::engine::Result::new(17, description);
    let translated = translate_observation(Ok(Outcome::Described(result)))
        .unwrap_or_else(|error| panic!("translate exact StreamsGroup description: {error}"));

    assert_eq!(translated.throttle_time().as_millis(), 17);
    let description = translated.description();
    assert_eq!(description.group_id(), "streams-workers");
    assert_eq!(
        description
            .topology()
            .and_then(|value| value.subtopologies())
            .map(len),
        Some(1)
    );
    assert_eq!(description.members()[0].instance_id(), Some("instance-a"));
    assert_eq!(description.members()[0].task_offsets()[0].offset(), 91);
    assert_eq!(
        description.members()[0].target_assignment().standby_tasks()[0].partitions(),
        [1]
    );
    assert_eq!(
        description.topology_description_status(),
        Some(StreamsGroupTopologyDescriptionStatus::from_raw(89))
    );
    let graph = description
        .topology_description()
        .unwrap_or_else(|| panic!("v1 graph must remain present"));
    assert_eq!(graph.subtopologies()[0].nodes()[0].node_type().as_raw(), 91);
    assert_eq!(graph.global_stores()[0].source().sink_topic(), None);
}

#[test]
fn selected_v0_status_absence_survives_exact_loopback() {
    let description = Description::new(
        "streams-workers".to_owned(),
        "Empty".to_owned(),
        1,
        1,
        Some(Topology::new(1, None)),
        Vec::new(),
        None,
        None,
        None,
    );
    let translated = translate_observation(Ok(Outcome::Described(super::engine::Result::new(
        0,
        description,
    ))))
    .unwrap_or_else(|error| panic!("translate v0 StreamsGroup description: {error}"));

    assert_eq!(translated.description().topology_description_status(), None);
    assert_eq!(
        translated
            .description()
            .topology()
            .and_then(|topology| topology.subtopologies()),
        None
    );
}

fn len<T>(values: &[T]) -> usize {
    values.len()
}
