//! Deliberately cloneable retained Fetch values for negative detector evidence.

#[derive(Clone, Copy)]
struct FetchResponse;

#[derive(Clone, Copy)]
struct FetchTopic;

#[derive(Clone, Copy)]
struct FetchPartition;

#[derive(Clone, Copy)]
struct FetchEndpoint;

#[derive(Clone, Copy)]
struct FetchBatch;

#[derive(Clone, Copy)]
struct FetchRecord;

#[derive(Clone, Copy)]
struct FetchHeader;

#[derive(Clone, Copy)]
struct FetchOutcome;

#[derive(Clone, Copy)]
struct RetainedFetchOutcome;

#[derive(Clone, Copy)]
struct RejectedFetchOutcome;

#[derive(Clone, Copy)]
struct FetchOutputReservation;

#[derive(Clone, Copy)]
struct FetchRetainedCharge;

#[derive(Clone, Copy)]
struct FetchReservationDomain;

#[derive(Clone, Copy)]
struct FetchStoreReservation;

#[derive(Clone, Copy)]
struct FetchStageProof;

#[derive(Clone, Copy)]
struct FetchSlot;

#[derive(Clone, Copy)]
struct FetchDeliveryStore;

#[derive(Clone, Copy)]
struct FetchDelivery;
