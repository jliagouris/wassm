use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Capability, Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::event::Date;

use super::maybe_refresh_faster;
use crate::queries::{NexmarkInput, NexmarkTimer};
use tempfile::TempDir;
use faster_rs::{FasterKv,status};
use std::collections::HashSet;

pub fn q7<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_size_ns: usize,
) -> Stream<S, usize> {
    input
        .bids(scope)
        .map(move |b| {
            (
                Date::new(((*b.date_time / window_size_ns) + 1) * window_size_ns),
                b.price,
            )
        })
        .unary_frontier(Pipeline, "Q7 Pre-reduce", |_cap, _info| {
            // Tracks the worker-local maximal bid for each capability.
            let state_directory = TempDir::new_in(".").unwrap().into_path();
            let state = FasterKv::new_u64_store(
                1 << 24,
                6 * 1024 * 1024 * 1024,
                state_directory.to_str().unwrap().to_string(),
            )
            .unwrap();
            let mut state_store_serial = 0;

            let mut capabilities = Vec::<Capability<usize>>::new();
            move |input, output| {
                input.for_each(|time, data| {
                    for (window, price) in data.iter().cloned() {
                        let (status, recv) = state.read_u64(nt.from_nexmark_time(window) as u64, state_store_serial);
                        if status == status::PENDING {
                            state.complete_pending(true);
                        }
                        maybe_refresh_faster(&state, &mut state_store_serial);
                        match recv.recv() {
                            Ok(current_highest) => {
                                if (current_highest as usize) < price {
                                    state.upsert_u64(nt.from_nexmark_time(window) as u64, price as u64, state_store_serial);
                                    maybe_refresh_faster(&state, &mut state_store_serial);
                                }
                            },
                            Err(_) => {
                                state.upsert_u64(nt.from_nexmark_time(window) as u64, price as u64, state_store_serial);
                                maybe_refresh_faster(&state, &mut state_store_serial);
                                capabilities.push(time.delayed(&nt.from_nexmark_time(window)));
                            },
                        }
                    }
                });

                for &(ref cap) in capabilities.iter() {
                    if !input.frontier.less_than(cap.time()) {
                        let (status, recv) = state.read_u64(*cap.time() as u64, state_store_serial);
                        if status == status::PENDING {
                            state.complete_pending(true);
                        }
                        maybe_refresh_faster(&state, &mut state_store_serial);
                        output
                            .session(&cap)
                            .give((*cap.time(), recv.recv().expect("Value must be present") as usize));
                    }
                }
                capabilities.retain(|capability| input.frontier.less_than(capability));
            }
        })
        .unary_frontier(
            Exchange::new(move |x: &(usize, usize)| (x.0 / window_size_ns) as u64),
            "Q7 All-reduce",
            |_cap, _info| {
                // Tracks the global maximal bid for each capability.
                let state_directory = TempDir::new_in(".").unwrap().into_path();
                let state = FasterKv::new_u64_store(
                    1 << 24,
                    2 * 1024 * 1024 * 1024,
                    state_directory.to_str().unwrap().to_string(),
                )
                    .unwrap();
                let mut state_store_serial = 0;

                let mut capabilities = Vec::<Capability<usize>>::new();
                move |input, output| {
                    input.for_each(|time, data| {
                        for (window, price) in data.iter().cloned() {
                            let (status, recv) = state.read_u64(window as u64, state_store_serial);
                            if status == status::PENDING {
                                state.complete_pending(true);
                            }
                            maybe_refresh_faster(&state, &mut state_store_serial);
                            match recv.recv() {
                                Ok(current_highest) => {
                                    if (current_highest as usize) < price {
                                        state.upsert_u64(window as u64, price as u64, state_store_serial);
                                        maybe_refresh_faster(&state, &mut state_store_serial);
                                    }
                                },
                                Err(_) => {
                                    state.upsert_u64(window as u64, price as u64, state_store_serial);
                                    maybe_refresh_faster(&state, &mut state_store_serial);
                                    capabilities.push(time.delayed(&window));
                                },
                            }
                        }
                    });

                    for &(ref cap) in capabilities.iter() {
                        if !input.frontier.less_than(cap.time()) {
                            let (status, recv) = state.read_u64(*cap.time() as u64, state_store_serial);
                            if status == status::PENDING {
                                state.complete_pending(true);
                            }
                            maybe_refresh_faster(&state, &mut state_store_serial);
                            output
                                .session(&cap)
                                .give(recv.recv().expect("Value must be present") as usize);
                        }
                    }
                    capabilities.retain(|capability| input.frontier.less_than(capability));
                }
            },
        )
}
