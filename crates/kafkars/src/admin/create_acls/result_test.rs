//! Exact caller-order and nullable ACL creation result tests.

use std::time::Duration;

use super::{
    super::{
        AccessControlEntry, AclBinding, AclOperation, AclPatternType, AclPermissionType,
        AclResourceType, ResourcePattern,
    },
    CreateAclBrokerError, CreateAclOutcome, CreateAclResult, CreateAclsResult,
};

#[test]
fn result_preserves_caller_order_signed_errors_and_nullable_diagnostics() {
    let outcomes = vec![
        CreateAclOutcome::new(binding("first"), CreateAclResult::Created),
        CreateAclOutcome::new(
            binding("second"),
            CreateAclResult::BrokerFailed(CreateAclBrokerError::new(-31_777, None, false)),
        ),
        CreateAclOutcome::new(
            binding("third"),
            CreateAclResult::BrokerFailed(CreateAclBrokerError::new(
                731,
                Some("denied".to_owned()),
                true,
            )),
        ),
    ];
    let result = CreateAclsResult::new(Duration::from_millis(19), outcomes);

    assert_eq!(result.throttle_time(), Duration::from_millis(19));
    assert_eq!(result.outcomes()[0].binding().pattern().name(), "first");
    assert_eq!(result.outcomes()[1].binding().pattern().name(), "second");
    let CreateAclResult::BrokerFailed(second) = result.outcomes()[1].result() else {
        panic!("broker failure expected");
    };
    assert_eq!(second.code(), -31_777);
    assert_eq!(second.message(), None);
    assert!(!second.message_truncated());

    let CreateAclResult::BrokerFailed(third) = result.outcomes()[2].result() else {
        panic!("broker failure expected");
    };
    assert_eq!(third.code(), 731);
    assert_eq!(third.message(), Some("denied"));
    assert!(third.message_truncated());
    assert_eq!(result.into_outcomes().len(), 3);
}

fn binding(name: &str) -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(AclResourceType::TOPIC, name, AclPatternType::LITERAL),
        AccessControlEntry::new(
            "User:alice",
            "*",
            AclOperation::READ,
            AclPermissionType::ALLOW,
        ),
    )
}
