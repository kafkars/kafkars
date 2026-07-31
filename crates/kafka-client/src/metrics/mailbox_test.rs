//! Public mailbox-metric accessor shape.

use super::mailbox::MailboxMetrics;

#[test]
fn mailbox_accessors_preserve_gauge_and_counter_shapes() {
    let _: fn(MailboxMetrics) -> usize = MailboxMetrics::capacity_per_lane;
    let _: fn(MailboxMetrics) -> usize = MailboxMetrics::byte_capacity_per_lane;
    let _: fn(MailboxMetrics) -> usize = MailboxMetrics::queued_work;
    let _: fn(MailboxMetrics) -> usize = MailboxMetrics::queued_work_bytes;
    let _: fn(MailboxMetrics) -> usize = MailboxMetrics::queued_control;
    let _: fn(MailboxMetrics) -> usize = MailboxMetrics::queued_control_bytes;
    let _: fn(MailboxMetrics) -> u64 = MailboxMetrics::work_full;
    let _: fn(MailboxMetrics) -> u64 = MailboxMetrics::work_byte_full;
    let _: fn(MailboxMetrics) -> u64 = MailboxMetrics::control_full;
    let _: fn(MailboxMetrics) -> u64 = MailboxMetrics::control_byte_full;
    let _: fn(MailboxMetrics) -> u64 = MailboxMetrics::closed_rejections;
    let _: fn(MailboxMetrics) -> u64 = MailboxMetrics::wake_failures;
}
