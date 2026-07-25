//! A second group offset-commit host construction site forbidden by this fixture.

struct SiblingGroupOwner;

impl SiblingGroupOwner {
    fn violate(&mut self) {
        self.start_group_offset_commit_host();
    }
}
