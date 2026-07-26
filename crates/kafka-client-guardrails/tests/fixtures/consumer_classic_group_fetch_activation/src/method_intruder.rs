//! Deliberate foreign use of group Fetch activation authority.

fn steal<T>(owner: &mut T) {
    owner.prepare_classic_group_fetch_activation();
    owner.install_resolved_assignment();
    owner.observe_effect();
    owner.arm_fetch();
    owner.observe_control();
}
