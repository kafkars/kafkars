//! Deliberate loss of post-core activation ownership through process failure.

fn steal<T>(transition: Option<T>) -> T {
    assert!(transition.is_some());
    let _ = transition.as_ref().unwrap();
    if transition.is_none() {
        panic!("lost transition");
    }
    if false {
        unreachable!("missing assignment epoch");
    }
    transition
        .expect("transition")
        .unwrap_or_else(|| panic!("transition"))
}
