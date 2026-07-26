//! Coordinator rediscovery gate installation, transfer, and recovery scenarios.

use super::classic_group_rediscovery::{
    ClassicCoordinatorRediscovery, ClassicCoordinatorRediscoveryError,
};

#[test]
fn rediscovery_blocks_join_from_core_install_until_driver_terminal_permission() {
    let mut rediscovery = ClassicCoordinatorRediscovery::new();
    let prepared = rediscovery
        .prepare_rediscovery_install()
        .unwrap_or_else(|error| panic!("gate installation failed: {error:?}"));
    prepared.commit();

    assert!(rediscovery.blocks_join());
    assert!(rediscovery.awaits_route_transfer());
    assert_eq!(rediscovery.unsettled(), 1);

    rediscovery
        .confirm_rediscovery_transfer()
        .unwrap_or_else(|error| panic!("route transfer failed: {error:?}"));
    assert!(rediscovery.blocks_join());
    assert!(!rediscovery.awaits_route_transfer());

    rediscovery
        .permit_rejoin()
        .unwrap_or_else(|error| panic!("terminal permission failed: {error:?}"));
    assert!(!rediscovery.blocks_join());
    assert_eq!(rediscovery.unsettled(), 0);
}

#[test]
fn invalid_transitions_preserve_the_existing_gate_state() {
    let mut rediscovery = ClassicCoordinatorRediscovery::new();
    assert_eq!(
        rediscovery.confirm_rediscovery_transfer(),
        Err(ClassicCoordinatorRediscoveryError::TransferNotPending)
    );
    assert_eq!(
        rediscovery.permit_rejoin(),
        Err(ClassicCoordinatorRediscoveryError::InvalidationNotPending)
    );

    rediscovery
        .prepare_rediscovery_install()
        .unwrap_or_else(|error| panic!("gate installation failed: {error:?}"))
        .commit();
    assert!(matches!(
        rediscovery.prepare_rediscovery_install(),
        Err(ClassicCoordinatorRediscoveryError::Occupied)
    ));
    assert!(rediscovery.awaits_route_transfer());
}

#[test]
fn only_post_driver_shutdown_recovery_reopens_an_unfinished_gate() {
    let mut rediscovery = ClassicCoordinatorRediscovery::new();
    rediscovery
        .prepare_rediscovery_install()
        .unwrap_or_else(|error| panic!("gate installation failed: {error:?}"))
        .commit();
    rediscovery
        .confirm_rediscovery_transfer()
        .unwrap_or_else(|error| panic!("route transfer failed: {error:?}"));

    rediscovery.clear_rediscovery_after_driver_shutdown();

    assert!(!rediscovery.blocks_join());
    assert_eq!(rediscovery.unsettled(), 0);
}
