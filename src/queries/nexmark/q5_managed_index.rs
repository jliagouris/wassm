use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

use std::str::FromStr;

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

pub fn q5_managed_index<S: Scope<Timestamp = usize>>(
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
                let mut state_index = state_handle.get_managed_map("index");
                let mut pre_reduce_state = state_handle.get_managed_map("state");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at end timestamp of the slide the current epoch corresponds to
                    let current_slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    let window_end = current_slide + (window_slice_count - 1) * window_slide_ns;
                    // Ask notification for the end of the window
                    notificator.notify_at(time.delayed(&window_end));
                    data.swap(&mut buffer);
                    for &(auction, a_time) in buffer.iter() {
                        if a_time != current_slide {
                            // a_time < current_slide
                            // Ask notification for the end of the latest window the record corresponds to
                            let w_end = a_time + (window_slice_count - 1) * window_slide_ns;
                            notificator.notify_at(time.delayed(&w_end));
                        }
                        let mut exists = false;
                        {   // Check if composite key exists in the slide
                            // println!("Composite Key: {:?}",(a_time,auction));
                            let keys: Option<std::rc::Rc<Vec<usize>>> = state_index.get(&a_time);
                            if keys.is_some() {
                                // println!("Composite keys: {:?}",keys);
                                let keys = keys.unwrap();
                                exists = keys.iter().any(|k: &usize| *k==auction);
                            }
                        }
                        if !exists {  // Insert new composite key
                            let mut keys = state_index.remove(&a_time).unwrap_or(Vec::new());
                            keys.push(auction);
                            state_index.insert(a_time, keys)
                        }
                        let composite_key = (a_time, auction);
                        let mut count = pre_reduce_state.remove(&composite_key).unwrap_or(0);
                        // println!("Composite key {:?} with count {}", composite_key, count);
                        count += 1;
                        // Index auction counts by composite key 'slide_auction'
                        pre_reduce_state.insert(composite_key, count);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // TODO (john): Use Prefix scan for RocksDB
                    // println!("Received notification for the end of window {}", cap.time());
                    let mut counts = HashMap::new();
                    for i in 0..window_slice_count {
                        let slide = cap.time() - i * window_slide_ns;
                        // println!("Slide: {}",slide);
                        if let Some(auction_ids) = state_index.get(&slide) {
                            for auction_id in auction_ids.iter() {
                                let composite_key = (slide, *auction_id);
                                // Look up state
                                let count = pre_reduce_state.get(&composite_key).expect("Composite key must exist");
                                // println!("Found auction id {} in composite key {:?}",auction_id,composite_key);
                                let c = counts.entry(auction_id.clone()).or_insert(0);
                                *c += *count;
                            }
                        }
                        else {
                            println!("Could not find index entry for window {}",cap.time());
                        }
                    }
                    if let Some((co, ac)) = counts.iter().map(|(&a, &c)| (c, a)).max() {
                        // Gives the accumulation per worker
                        output.session(&cap).give((ac, co));
                    }
                    // Remove the first slide of the expired window
                    let slide_to_remove = cap.time() - (window_slice_count - 1) * window_slide_ns;
                    // println!("Slide to remove: {}",slide_to_remove);
                    if let Some(auctions_in_slide) = state_index.get(&slide_to_remove) {
                        // println!("Auctions to remove: {:?}",auctions_in_slide);
                        state_index.remove(&slide_to_remove);
                        for auction in auctions_in_slide.iter() {
                            pre_reduce_state.remove(&(slide_to_remove,*auction));
                        }
                    }
                    else {
                        println!("End of window {}. Could not find slide {}",cap.time(),slide_to_remove);
                    }
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
