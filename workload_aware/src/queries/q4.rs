use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use super::maybe_refresh_faster;
use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::{status, FasterKv};
use tempfile::TempDir;

pub fn q4<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (usize, usize)> {
    let aggs_directory = TempDir::new_in(".").unwrap().into_path();
    // Stores category -> (total, count)
    let aggs = FasterKv::new_u64_pair_store(
        1 << 24,
        512 * 1024 * 1024,
        aggs_directory.to_str().unwrap().to_string(),
    )
    .unwrap();
    let mut aggs_store_serial = 0;
    input
        .closed_auctions(scope)
        .map(|(a, (_, b))| (a, b))
        .unary(
            Exchange::new(|x: &(usize, usize)| x.0 as u64),
            "Q4 Average",
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for (category, price) in data.iter().cloned() {
                            let (res, recv) =
                                aggs.read_u64_pair(category as u64, aggs_store_serial);
                            if res == status::PENDING {
                                aggs.complete_pending(true);
                            }
                            maybe_refresh_faster(&aggs, &mut aggs_store_serial);
                            let avg = match recv.recv() {
                                Ok((sum, count)) => {
                                    *sum += (price as u64);
                                    *count += 1;
                                    *sum / *count
                                }
                                Err(_) => {
                                    aggs.upsert_u64_pair(
                                        category as u64,
                                        (price as u64, 1),
                                        aggs_store_serial,
                                    );
                                    maybe_refresh_faster(&aggs, &mut aggs_store_serial);
                                    price as u64
                                }
                            };
                            session.give((category, avg as usize));
                        }
                    })
                }
            },
        )
}
