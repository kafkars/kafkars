//! Singular ShareGroup description Admin entry-point surface test.

use crate::admin::{Admin, DescribeShareGroupBuilder};

#[test]
fn entry_point_builds_one_inert_singular_request() {
    fn assert_entry_point<F>(_entry_point: F)
    where
        F: for<'a> Fn(&'a Admin, String) -> DescribeShareGroupBuilder,
    {
    }

    assert_entry_point(|admin: &Admin, group_id: String| admin.describe_share_group(group_id));
}
