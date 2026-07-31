//! Fetch-session request identity and epoch progression scenarios.

use super::session::FetchSessionRequest;

#[test]
fn only_positive_incremental_identity_and_epoch_are_admitted() {
    assert_eq!(FetchSessionRequest::incremental(0, 1), None);
    assert_eq!(FetchSessionRequest::incremental(1, 0), None);
    let request = FetchSessionRequest::incremental(91, 3)
        .unwrap_or_else(|| panic!("positive session identity"));
    assert_eq!((request.session_id(), request.session_epoch()), (91, 3));
    assert!(request.is_incremental());
}

#[test]
fn incremental_epoch_wraps_to_one_without_using_control_sentinels() {
    let request = FetchSessionRequest::incremental(91, i32::MAX)
        .unwrap_or_else(|| panic!("maximum incremental epoch"));
    assert_eq!(request.next_incremental_epoch(), Some(1));
    assert_eq!(FetchSessionRequest::INITIAL.next_incremental_epoch(), None);
    assert_eq!(FetchSessionRequest::LEGACY.next_incremental_epoch(), None);
}

#[test]
fn only_live_incremental_metadata_can_form_a_final_epoch_close() {
    assert_eq!(FetchSessionRequest::LEGACY.close(), None);
    assert_eq!(FetchSessionRequest::INITIAL.close(), None);
    let live = FetchSessionRequest::incremental(91, 3)
        .unwrap_or_else(|| panic!("positive incremental session"));
    let close = live.close().unwrap_or_else(|| panic!("live session close"));
    assert_eq!((close.session_id(), close.session_epoch()), (91, -1));
    assert!(close.is_close());
    assert!(!close.is_incremental());
}
