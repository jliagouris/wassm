use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use super::maybe_refresh_faster;
use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::{status, FasterKv};
use tempfile::TempDir;
use std::collections::HashMap;

pub fn q4_flex<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (usize, usize)> {
    // Stores category -> (total, count)
    let mut state = HashMap::new();
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
