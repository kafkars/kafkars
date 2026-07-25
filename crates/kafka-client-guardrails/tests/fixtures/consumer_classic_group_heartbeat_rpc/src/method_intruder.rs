//! Deliberate foreign use of classic Heartbeat tracked-call methods.

fn steal<T>(owner: T) {
    owner.submit_tracked_classic_heartbeat();
    owner.try_reserve_classic_heartbeat();
    owner.poll_classic_heartbeat();
    owner.begin_classic_heartbeat_settlement();
    owner.restore_classic_heartbeat_settlement();
    owner.confirm_classic_heartbeat_settlement();
    owner.reconcile_classic_heartbeat_after_driver_shutdown();
    owner.confirm_classic_heartbeat_call_receipt();
    owner.confirm_classic_heartbeat_route_token();
    owner.consume_classic_heartbeat_shutdown_receipt();
    owner.retained_classic_heartbeat_count();
    owner.pop_active();
    owner.take_settled();
    owner.take_pending();
    owner.take_completion();
}
