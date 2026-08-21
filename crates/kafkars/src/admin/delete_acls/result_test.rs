//! Positional duplicate-filter and nested matched-binding result tests.

use std::time::Duration;

use super::{
    super::{
        AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
        AclPermissionType, AclResourceType, ResourcePattern,
    },
    DeleteAclBrokerError, DeleteAclFilterOutcome, DeleteAclFilterResult, DeleteAclMatchOutcome,
    DeleteAclMatchResult, DeleteAclsResult,
};

#[test]
fn duplicate_filter_positions_and_nested_order_remain_exact() {
    let duplicate = AclBindingFilter::any().with_resource_name("orders");
    let result = DeleteAclsResult::new(
        Duration::from_millis(23),
        vec![
            DeleteAclFilterOutcome::new(
                duplicate.clone(),
                DeleteAclFilterResult::Matched(vec![
                    DeleteAclMatchOutcome::new(binding("first"), DeleteAclMatchResult::Deleted),
                    DeleteAclMatchOutcome::new(
                        binding("second"),
                        DeleteAclMatchResult::BrokerFailed(DeleteAclBrokerError::new(
                            -731, None, false,
                        )),
                    ),
                ]),
            ),
            DeleteAclFilterOutcome::new(
                duplicate,
                DeleteAclFilterResult::BrokerFailed(DeleteAclBrokerError::new(
                    17,
                    Some("filter denied".to_owned()),
                    true,
                )),
            ),
        ],
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(23));
    assert_eq!(result.outcomes()[0].filter(), result.outcomes()[1].filter());
    let DeleteAclFilterResult::Matched(matches) = result.outcomes()[0].result() else {
        panic!("matched result expected");
    };
    assert_eq!(matches[0].binding().pattern().name(), "first");
    assert_eq!(matches[1].binding().pattern().name(), "second");
    let DeleteAclMatchResult::BrokerFailed(error) = matches[1].result() else {
        panic!("binding failure expected");
    };
    assert_eq!(error.code(), -731);
    assert_eq!(error.message(), None);

    let DeleteAclFilterResult::BrokerFailed(error) = result.outcomes()[1].result() else {
        panic!("filter failure expected");
    };
    assert_eq!(error.code(), 17);
    assert_eq!(error.message(), Some("filter denied"));
    assert!(error.message_truncated());
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
