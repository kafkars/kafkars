//! Deliberate foreign use of local position close authority.

fn steal<T>(owner: &mut T) {
    owner.try_submit_group_position_offset_fetch();
    owner.begin_handoff();
    owner.restore_prepared();
    owner.confirm_driver_owned();
    owner.finish_driver_rejected();
    owner.apply_raw_terminal();
    owner.confirm_terminal_settlement();
    owner.close_position_if_local();
    owner.expire_prepared_if_due();
    owner.recover_key_after_driver_shutdown();
    owner.recover_terminal_after_driver_shutdown();
    owner.recover_confirmation_after_driver_shutdown();
}
