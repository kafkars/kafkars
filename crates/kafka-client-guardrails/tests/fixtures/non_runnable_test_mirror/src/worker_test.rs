//! Test half containing only ignored or conditionally disabled evidence.

#[test]
#[ignore = "fixture proves ignored evidence is insufficient"]
fn ignored_evidence() {}

#[cfg(any())]
#[test]
fn disabled_evidence() {}

#[cfg(any())]
mod disabled_group {
    #[test]
    fn nested_disabled_evidence() {}
}

fn local_scope_is_not_test_evidence() {
    #[test]
    fn nested_decoy() {}
}

const _: () = {
    #[test]
    fn const_block_decoy() {}
};
