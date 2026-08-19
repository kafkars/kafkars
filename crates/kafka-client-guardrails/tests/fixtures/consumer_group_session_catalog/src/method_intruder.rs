//! Forbidden indirect installation of staged group-session state.

struct SiblingGroupOwner;

impl SiblingGroupOwner {
    fn violate(&mut self) {
        self.commit_classic_group_install();
        self.commit_classic_group_revoke();
        self.from_prepared_member_with_owned();
        self.try_from_prepared_cycle();
        self.into_catalog_install();
    }
}
