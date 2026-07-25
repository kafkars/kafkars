//! Forbidden indirect installation of staged group-session state.

struct SiblingGroupOwner;

impl SiblingGroupOwner {
    fn violate(&mut self) {
        self.install_group_session_replacement();
    }
}
