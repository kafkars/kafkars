//! ShareGroup offset-alteration Admin entry-point surface tests.

use crate::admin::{Admin, AlterShareGroupOffsetsBuilder, ShareGroupOffsetAlteration};

#[test]
fn entry_point_builds_one_inert_caller_ordered_request() {
    fn assert_entry_point<F>(_entry_point: F)
    where
        F: for<'a> Fn(
            &'a Admin,
            String,
            Vec<ShareGroupOffsetAlteration>,
        ) -> AlterShareGroupOffsetsBuilder,
    {
    }

    assert_entry_point(
        |admin: &Admin, group_id: String, alterations: Vec<ShareGroupOffsetAlteration>| {
            admin.alter_share_group_offsets(group_id, alterations)
        },
    );
}
