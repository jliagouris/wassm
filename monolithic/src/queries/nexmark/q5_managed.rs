use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

#[derive(Deserialize, Serialize)]
struct Counts(HashMap<usize, usize>);

impl FasterRmw for Counts {
    fn rmw(&self, _modification: Self) -> Self {
        panic!("RMW on Counts not allowed!");
    }
}

#[derive(Deserialize, Serialize)]
struct AuctionBids((usize, usize));

impl FasterRmw for AuctionBids {
    fn rmw(&self, _modification: Self) -> Self {
        panic!("RMW on AuctionBids not allowed!");
    }
}

pub fn q5_managed<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, usize> {
    input
        .bids(scope)
        .map(move |b| {
            (
                b.auction,
                // The end timestamp of the slide the current event corresponds to
                ((*b.date_time / window_slide_ns) + 1) * window_slide_ns,
            )
        })
        // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Q5 Accumulate Per Worker",
            None,
            move |input, output, notificator, state_handle| {
                let mut pre_reduce_state = state_handle.get_managed_map("state");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at end timestamp of the slide the current epoch corresponds to
                    let current_slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    let window_end = current_slide + (window_slice_count - 1) * window_slide_ns;
                    // Ask notification for the end of the window
                    notificator.notify_at(time.delayed(&window_end));
                    // Notify when data exits window
                    //notificator.notify_at(time.delayed(
                    //    &(((time.time() / window_slide_ns) + 1 + window_slice_count)
                    //        * window_slide_ns),
                    //));
                    data.swap(&mut buffer);
                    for &(auction, a_time) in buffer.iter() {
                        if a_time != current_slide {
                            // Ask notification for the end of the latest window the record corresponds to
                            let w_end = a_time + (window_slice_count - 1) * window_slide_ns;
                            notificator.notify_at(time.delayed(&w_end));
                        }
                        let mut counts: Counts = pre_reduce_state
                            .remove(&a_time)
                            .unwrap_or(Counts(HashMap::new()));
                        let count = counts.0.entry(auction).or_insert(0);
                        *count += 1;
                        // Index auction counts by the end timestamp of the slide they correspond to
                        pre_reduce_state.insert(a_time, counts);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // Received notification for the end of window
                    let mut counts = HashMap::new();
                    for i in 0..window_slice_count {
                        if let Some(slide_counts) =
                            pre_reduce_state.get(&(cap.time() - i * window_slide_ns))
                        {
                            for (auction, count) in slide_counts.0.iter() {
                                *counts.entry(*auction).or_insert(0) += *count;
                            }
                        }
                    }
                    if let Some((co, ac)) = counts.iter().map(|(&a, &c)| (c, a)).max() {
                        // Gives the accumulation per worker
                        output.session(&cap).give((ac, co));
                    }
                    // Remove the last first slide of the expired window
                    pre_reduce_state.remove(&(cap.time() - window_slice_count * window_slide_ns));
                });
            },
        )
        .unary_notify(
            Exchange::new(|_| 0),
            "Q5 Accumulate Globally",
            None,
            move |input, output, notificator, state_handle| {
                let mut all_reduce_state = state_handle.get_managed_map("state");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Ask notification at the end of the window to produce output and clean up state
                    notificator.notify_at(time.delayed(&(time.time())));
                    data.swap(&mut buffer);
                    for &(auction_id, count) in buffer.iter() {
                        let current_item = all_reduce_state.get(time.time());
                        match current_item {
                            None => all_reduce_state
                                .insert(*time.time(), AuctionBids((auction_id, count))),
                            Some(current_item) => {
                                if count > (current_item.0).1 {
                                    all_reduce_state
                                        .insert(*time.time(), AuctionBids((auction_id, count)));
                                }
                            }
                        }
                    }
                });
                notificator.for_each(|cap, _, _| {
                    output
                        .session(&cap)
                        .give((all_reduce_state.remove(cap.time()).expect("Must exist").0).0)
                });
            },
        )
}