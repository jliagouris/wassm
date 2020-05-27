use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};

pub fn q4<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (usize, usize)> {
    input
        .closed_auctions(scope)
        .map(|(a, b)| (a.category, b.price))
        .unary(
            Exchange::new(|x: &(usize, usize)| x.0 as u64),
            "Q4 Average",
            |_cap, _info, _state_handle| {
                // Stores category -> (total, count)
                let mut state = std::collections::HashMap::new();

                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for (category, price) in data.iter().cloned() {
                            let entry = state.entry(category).or_insert((0, 0));
                            entry.0 += price;
                            entry.1 += 1;
                            session.give((category, entry.0 / entry.1));
                        }
                    })
                }
            },
        )
}
