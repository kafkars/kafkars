//! Raw-terminal mismatch retention before core settlement.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AlterClientQuotaEntity, AlterClientQuotaEntityComponent, AlterClientQuotaEntry,
    AlterClientQuotaOperation, AlterClientQuotasInput, AlterClientQuotasPlan, Moment,
};

use crate::{admin::AdminCompletionNotifier, clock::OperationDeadline};

use super::super::super::{
    AlterClientQuotasHost, AlterClientQuotasHostError, AlterClientQuotasTurn,
};

#[test]
fn mismatched_raw_terminal_cannot_settle_core_or_publish() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut host = AlterClientQuotasHost::new(ports.alter_client_quotas);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit alteration: {error:?}"));
    let AlterClientQuotasTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _plan, retained_limit) = submission.into_parts();
    host.apply_input_for_test(operation_id, AlterClientQuotasInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept synthetic driver ownership: {error}"));
    host.retain_raw_terminal_for_test(plan("bob"), retained_limit);

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(AlterClientQuotasHostError::SubmissionMismatch)
    ));
    assert!(host.raw_terminal_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterClientQuotasHostError::InvalidHandoff)
    ));

    drop((admission, host));
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}

fn plan(name: &str) -> AlterClientQuotasPlan {
    AlterClientQuotasPlan::new(
        vec![AlterClientQuotaEntry::new(
            AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
                "user".to_owned(),
                Some(name.to_owned()),
            )]),
            vec![AlterClientQuotaOperation::set(
                "producer_byte_rate".to_owned(),
                4096.0,
            )],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid alteration plan: {error}"))
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    )
}
