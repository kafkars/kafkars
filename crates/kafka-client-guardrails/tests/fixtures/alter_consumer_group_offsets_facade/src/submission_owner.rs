//! Sole allowed private bridge submission owner.

fn submit<T>(engine: &T) {
    engine.try_alter_consumer_group_offsets();
}
