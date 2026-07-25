//! Deliberate foreign use of hosted classic Heartbeat transition methods.

fn steal<T>(owner: T) {
    owner.prepare_one_classic_heartbeat();
    owner.expire_one_prepared_heartbeat();
    owner.submit_one_classic_heartbeat();
    owner.settle_one_classic_heartbeat();
    owner.recover_classic_heartbeats_after_driver_shutdown();
    owner.prepare_install();
}
