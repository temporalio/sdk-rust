extern crate prost_descriptor as prost_types;

use prost::Message;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    mem::size_of,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct Counter;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if ACTIVE.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(l.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if ACTIVE.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(l.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if ACTIVE.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(n, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}
#[global_allocator]
static ALLOCATOR: Counter = Counter;
#[allow(dead_code)]
mod baseline {
    include!(concat!(env!("OUT_DIR"), "/baseline/mod.rs"));
}
#[allow(dead_code)]
mod optimized {
    include!(concat!(env!("OUT_DIR"), "/optimized/mod.rs"));
}
fn measure(mut f: impl FnMut()) -> (usize, usize) {
    f();
    CALLS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Relaxed);
    for _ in 0..1000 {
        f();
    }
    ACTIVE.store(false, Ordering::Relaxed);
    let c = CALLS.load(Ordering::Relaxed);
    let b = BYTES.load(Ordering::Relaxed);
    assert_eq!(c % 1000, 0);
    assert_eq!(b % 1000, 0);
    (c / 1000, b / 1000)
}
macro_rules! scenario {
    ($name:ident,$m:ident) => {
        mod $name {
            use super::*;
            use $m::temporal::api::{
                common::v1::{Payload, Payloads},
                failure::v1::Failure,
                update::v1::{
                    Acceptance, Input, Meta, Outcome, Rejection, Request, Response, outcome,
                },
            };
            #[allow(dead_code)]
            pub enum UpdateMsg {
                Accept(Acceptance),
                Reject(Rejection),
                Response(Response),
            }
            fn request() -> Request {
                Request {
                    meta: Some(meta()),
                    input: Some(Input {
                        name: "handler".into(),
                        args: Some(payloads()),
                        ..Default::default()
                    }),
                    request_id: "request-id".into(),
                    ..Default::default()
                }
            }
            fn meta() -> Meta {
                Meta {
                    update_id: "update-id".into(),
                    identity: "worker".into(),
                }
            }
            fn payloads() -> Payloads {
                Payloads {
                    payloads: vec![Payload {
                        data: b"populated-payload".to_vec(),
                        ..Default::default()
                    }],
                }
            }
            fn failure() -> Failure {
                Failure {
                    message: "failed".into(),
                    source: "RustSDK".into(),
                    stack_trace: "stack".into(),
                    cause: Some(Box::new(Failure {
                        message: "cause".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }
            }
            pub fn make(k: usize) -> UpdateMsg {
                match k {
                    0 => UpdateMsg::Accept(Acceptance {
                        accepted_request_message_id: "message-id".into(),
                        accepted_request_sequencing_event_id: 42,
                        accepted_request: Some(request()),
                    }),
                    1 => UpdateMsg::Reject(Rejection {
                        rejected_request_message_id: "message-id".into(),
                        rejected_request_sequencing_event_id: 42,
                        rejected_request: Some(request()),
                        failure: Some(failure().into()),
                    }),
                    _ => UpdateMsg::Response(Response {
                        meta: Some(meta()),
                        outcome: Some(Outcome {
                            value: Some(if k == 2 {
                                outcome::Value::Success(payloads())
                            } else {
                                outcome::Value::Failure(failure().into())
                            }),
                        }),
                    }),
                }
            }
            pub fn encode(m: &UpdateMsg) -> prost_descriptor::Any {
                let (name, value) = match m {
                    UpdateMsg::Accept(v) => ("Acceptance", v.encode_to_vec()),
                    UpdateMsg::Reject(v) => ("Rejection", v.encode_to_vec()),
                    UpdateMsg::Response(v) => ("Response", v.encode_to_vec()),
                };
                prost_descriptor::Any {
                    type_url: format!("type.googleapis.com/temporal.api.update.v1.{name}"),
                    value,
                }
            }
            pub fn decode(k: usize, wire: &[u8]) -> UpdateMsg {
                match k {
                    0 => UpdateMsg::Accept(Acceptance::decode(wire).unwrap()),
                    1 => UpdateMsg::Reject(Rejection::decode(wire).unwrap()),
                    _ => UpdateMsg::Response(Response::decode(wire).unwrap()),
                }
            }
        }
    };
}
scenario!(base, baseline);
scenario!(opt, optimized);
fn main() {
    println!(
        "UpdateMsg inline bytes baseline={} optimized={}",
        size_of::<base::UpdateMsg>(),
        size_of::<opt::UpdateMsg>()
    );
    println!(
        "branch,phase,baseline_calls,optimized_calls,delta_calls,baseline_requested_bytes,optimized_requested_bytes,delta_bytes"
    );
    let mut deltas = [[(0isize, 0isize); 2]; 4];
    for (k, name) in [
        "acceptance",
        "rejection",
        "response_success",
        "response_failure",
    ]
    .iter()
    .enumerate()
    {
        let b = base::encode(&base::make(k));
        let o = opt::encode(&opt::make(k));
        assert_eq!(b, o);
        assert_eq!(base::encode(&base::decode(k, &o.value)), b);
        assert_eq!(opt::encode(&opt::decode(k, &b.value)), o);
        for phase in 0..2 {
            let (bc, bb) = if phase == 0 {
                measure(|| {
                    let m = black_box(base::make(k));
                    drop(black_box(base::encode(&m)));
                    drop(m);
                })
            } else {
                measure(|| drop(black_box(base::decode(k, black_box(&b.value)))))
            };
            let (oc, ob) = if phase == 0 {
                measure(|| {
                    let m = black_box(opt::make(k));
                    drop(black_box(opt::encode(&m)));
                    drop(m);
                })
            } else {
                measure(|| drop(black_box(opt::decode(k, black_box(&b.value)))))
            };
            deltas[k][phase] = (oc as isize - bc as isize, ob as isize - bb as isize);
            println!(
                "{name},{},{bc},{oc},{},{bb},{ob},{}",
                if phase == 0 {
                    "construct_encode_Any_drop"
                } else {
                    "decode_drop"
                },
                deltas[k][phase].0,
                deltas[k][phase].1
            );
        }
    }
    println!(
        "Synthetic per-update mixes: r=rejected fraction; f=failure fraction among accepted; delta optimized-baseline"
    );
    println!("r,f,phase,expected_delta_calls,expected_delta_requested_bytes");
    for r in [0., 0.01, 0.1, 0.5, 1.] {
        for f in [0., 0.01, 0.1, 0.5, 1.] {
            for p in 0..2 {
                let d = |i: usize, j: usize| {
                    if j == 0 {
                        deltas[i][p].0 as f64
                    } else {
                        deltas[i][p].1 as f64
                    }
                };
                let e = |j| r * d(1, j) + (1. - r) * (d(0, j) + (1. - f) * d(2, j) + f * d(3, j));
                println!("{r},{f},{p},{:.4},{:.4}", e(0), e(1));
            }
        }
    }
}
