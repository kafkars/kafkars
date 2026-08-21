//! Inert `ShareGroup` offset-alteration request bridge tests.

use crate::admin::ShareGroupOffsetAlteration;

use super::{
    engine::{Request as EngineRequest, Target as EngineTarget},
    request::AlterShareGroupOffsetsAdminRequest,
};

#[test]
fn request_is_linear_sendable_and_preserves_caller_order() {
    fn assert_send<T: Send>() {}
    assert_send::<AlterShareGroupOffsetsAdminRequest>();

    let request = AlterShareGroupOffsetsAdminRequest::new(
        "workers".to_owned(),
        vec![
            ShareGroupOffsetAlteration::new("orders", 7, 42),
            ShareGroupOffsetAlteration::new("audit", 1, 3),
        ],
    );
    assert!(format!("{request:?}").contains("orders"));
    assert_eq!(
        request.into_engine(),
        EngineRequest::new(
            "workers".to_owned(),
            vec![
                EngineTarget::new("orders".to_owned(), 7, 42),
                EngineTarget::new("audit".to_owned(), 1, 3),
            ],
        ),
    );
}
