//! ShareGroup offset-deletion Admin entry-point surface tests.

use crate::admin::{Admin, DeleteShareGroupOffsetsBuilder};

#[test]
fn entry_point_builds_one_inert_request() {
    fn assert_entry_point<F>(_entry_point: F)
    where
        F: for<'a> Fn(&'a Admin, String, Vec<String>) -> DeleteShareGroupOffsetsBuilder,
    {
    }

    assert_entry_point(|admin: &Admin, group_id: String, topics: Vec<String>| {
        admin.delete_share_group_offsets(group_id, topics)
    });
}
