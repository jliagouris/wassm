use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

use std::str::FromStr;
use tempfile::TempDir;
use faster_rs::{FasterKv, status};

pub fn q5_index<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, usize> {
    let pre_reduce_state_directory = TempDir::new_in(".").unwrap().into_path();
    let pre_reduce_state = FasterKv::new_u64_composite_store(
        1 << 24,
        2 * 1024 * 1024 * 1024,
        pre_reduce_state_directory.to_str().unwrap().to_string(),
    )
        .unwrap();
    let mut pre_reduce_state_store_serial = 0;
    let hot_items_directory = TempDir::new_in(".").unwrap().into_path();
    let hot_items = FasterKv::new_u64_pair_store(
        1 << 24,
        2 * 1024 * 1024 * 1024,
        hot_items_directory.to_str().unwrap().to_string(),
    )
        .unwrap();
    let mut hot_items_store_serial = 0;
    let index_state_directory = TempDir::new_in(".").unwrap().into_path();
    let index_state = FasterKv::new_auctions_store(
        1 << 24,
        2 * 1024 * 1024 * 1024,
        index_state_directory.to_str().unwrap().to_string(),
    )
        .unwrap();
    let mut index_state_store_serial = 0;
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
            move |input, output, notificator| {
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
                            let (res, recv) = index_state.read_auctions(a_time as u64, index_state_store_serial);
                            if res == status::PENDING {
                                index_state.complete_pending(true);
                            }
                            super::maybe_refresh_faster(&index_state, &mut index_state_store_serial);
                            let keys: Option<&[u64]> = recv.recv().ok();
                            if keys.is_some() {
                                // println!("Composite keys: {:?}",keys);
                                let keys = keys.unwrap();
                                exists = keys.iter().any(|k: &u64| *k==(auction as u64));
                            }
                        }
                        if !exists {  // Insert new composite key
                            index_state.rmw_auction(a_time as u64, auction as u64, index_state_store_serial);
                            super::maybe_refresh_faster(&index_state, &mut index_state_store_serial);
                        }
                        let composite_key = (a_time as u64, auction as u64);
                        pre_reduce_state.rmw_u64_composite(composite_key, 1, pre_reduce_state_store_serial);
                        super::maybe_refresh_faster(&pre_reduce_state, &mut pre_reduce_state_store_serial);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let mut counts = HashMap::new();
                    for i in 0..window_slice_count {
                        let slide = cap.time() - i * window_slide_ns;
                        let (res, recv) = index_state.read_auctions(slide as u64, index_state_store_serial);
                        if res == status::PENDING {
                            index_state.complete_pending(true);
                        }
                        super::maybe_refresh_faster(&index_state, &mut index_state_store_serial);
                        if let Some(auction_ids) = recv.recv().ok() {
                            for auction_id in auction_ids.iter() {
                                let composite_key = (slide as u64, *auction_id as u64);
                                // Look up state
                                let (res, recv) = pre_reduce_state.read_u64_composite(composite_key, pre_reduce_state_store_serial);
                                if res == status::PENDING {
                                    pre_reduce_state.complete_pending(true);
                                }
                                super::maybe_refresh_faster(&pre_reduce_state, &mut pre_reduce_state_store_serial);
                                let c = counts.entry(auction_id.clone()).or_insert(0);
                                *c += recv.recv().unwrap();
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
                    let slide_to_remove: usize = cap.time() - (window_slice_count - 1) * window_slide_ns;
                    let (res, recv) = index_state.read_auctions(slide_to_remove as u64, index_state_store_serial);
                    if res == status::PENDING {
                        index_state.complete_pending(true);
                    }
                    super::maybe_refresh_faster(&index_state, &mut index_state_store_serial);
                    if let Some(auctions_in_slide) = recv.recv().ok() {
                        index_state.delete_auctions(slide_to_remove as u64, index_state_store_serial);
                        super::maybe_refresh_faster(&index_state, &mut index_state_store_serial);
                        for auction in auctions_in_slide.iter() {
                            pre_reduce_state.delete_u64_composite((slide_to_remove as u64, *auction as u64), pre_reduce_state_store_serial);
                            super::maybe_refresh_faster(&pre_reduce_state, &mut pre_reduce_state_store_serial);
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
            "Q5 All-Accumulate",
            None,
            move |input, output, notificator| {
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    let (status, recv) =
                        hot_items.read_u64_pair(*time.time() as u64, hot_items_store_serial);
                    if status == status::PENDING {
                        hot_items.complete_pending(true);
                    }
                    super::maybe_refresh_faster(&hot_items, &mut hot_items_store_serial);
                    let mut current_hottest = match recv.recv() {
                        Ok((left, right)) => (*left, *right),
                        Err(_) => (0, 0),
                    };
                    for &(auction, count) in buffer.iter() {
                        if count > current_hottest.1 {
                            current_hottest = (auction as u64, count);
                        }
                    }
                    hot_items.upsert_u64_pair(
                        *time.time() as u64,
                        current_hottest,
                        hot_items_store_serial,
                    );
                    super::maybe_refresh_faster(&hot_items, &mut hot_items_store_serial);
                    notificator.notify_at(time.delayed(&time.time()))
                });

                notificator.for_each(|cap, _, _| {
                    let (status, recv) =
                        hot_items.read_u64_pair(*cap.time() as u64, hot_items_store_serial);
                    if status == status::PENDING {
                        hot_items.complete_pending(true);
                    }
                    super::maybe_refresh_faster(&hot_items, &mut hot_items_store_serial);
                    output.session(&cap).give(*recv.recv().unwrap().0 as usize);
                });
            },
        )
}
