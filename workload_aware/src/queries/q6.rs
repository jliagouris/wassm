use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use super::maybe_refresh_faster;
use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::{status, FasterKv};
use tempfile::TempDir;

pub fn q6<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (usize, usize)> {
    let aggs_directory = TempDir::new_in(".").unwrap().into_path();
    // Store bidder -> [prices; 10]
    let aggs = FasterKv::new_u64_pair_store(
        1 << 24,
        512 * 1024 * 1024,
        aggs_directory.to_str().unwrap().to_string(),
    )
    .unwrap();
    let mut aggs_store_serial = 0;
    input.closed_auctions(scope).map(|(_a, b)| b).unary(
        Exchange::new(|x: &(usize, usize)| x.0 as u64),
        "Q6 Average",
        |_cap, _info| {
            move |input, output| {
                input.for_each(|time, data| {
                    let mut session = output.session(&time);
                    for (bidder, price) in data.iter().cloned() {
                        aggs.rmw_ten_elements(bidder as u64, price, aggs_store_serial);
                        maybe_refresh_faster(&aggs, &mut aggs_store_serial);
                        let (res, recv) =
                            aggs.read_ten_elements_average(bidder as u64, aggs_store_serial);
                        if res == status::PENDING {
                            aggs.complete_pending(true);
                        }
                        maybe_refresh_faster(&aggs, &mut aggs_store_serial);
                        session.give((bidder, recv.recv().unwrap()));
                    }
                });
            }
        },
    )
}
