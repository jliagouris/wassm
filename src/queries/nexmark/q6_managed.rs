use faster_rs::FasterRmw;
use std::collections::VecDeque;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};

#[derive(Serialize, Deserialize)]
struct Prices(VecDeque<usize>);

impl FasterRmw for Prices {
    fn rmw(&self, _modification: Self) -> Self {
        panic!("RMW on VecDeque<T> is unsafe");
    }
}

pub fn q6_managed<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (usize, usize)> {
    input
        .closed_auctions(scope)
        .map(|(_a, b)| (b.bidder, b.price))
        .unary(
            Exchange::new(|x: &(usize, usize)| x.0 as u64),
            "Q6 Average",
            |_cap, _info, state_handle| {
                // Store bidder -> [prices; 10]
                let mut state = state_handle.get_managed_map("state");

                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for (bidder, price) in data.iter().cloned() {
                            let mut entry =
                                state.remove(&bidder).unwrap_or(Prices(VecDeque::new())).0;
                            if entry.len() >= 10 {
                                entry.pop_back();
                            }
                            entry.push_front(price);
                            let sum: usize = entry.iter().sum();
                            session.give((bidder, sum / entry.len()));
                            state.insert(bidder, Prices(entry));
                        }
                    });
                }
            },
        )
}
