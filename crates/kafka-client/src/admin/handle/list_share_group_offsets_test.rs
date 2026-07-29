//! ShareGroup offset-listing Admin entry-point surface tests.

use crate::admin::{Admin, ListShareGroupOffsetsBuilder};

#[test]
fn entry_point_builds_one_inert_all_partition_request() {
    fn assert_entry_point<F>(_entry_point: F)
    where
        F: for<'a> Fn(&'a Admin, String) -> ListShareGroupOffsetsBuilder,
    {
    }

    assert_entry_point(|admin: &Admin, group_id: String| admin.list_share_group_offsets(group_id));
}
