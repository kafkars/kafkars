//! Exact owner of incremental configuration tracked-call execution.

fn drive<T>(calls: &mut T) {
    let _permit = calls.try_reserve();
    let _terminal = calls.poll_next_ready();
    calls.discard_settled();
}
