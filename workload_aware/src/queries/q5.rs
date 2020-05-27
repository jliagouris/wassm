use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::event::Date;

use super::maybe_refresh_faster;
use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::{status, FasterKv};
use tempfile::TempDir;

pub fn q5<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, usize> {
    //let mut additions = HashMap::new();
    let additions_directory = TempDir::new_in(".").unwrap().into_path();
    let additions = FasterKv::new_auctions_store(
        1 << 24,
        6 * 1024 * 1024 * 1024,
        additions_directory.to_str().unwrap().to_string(),
    )
    .unwrap();
    let mut additions_store_serial = 0;
    //let mut deletions = HashMap::new();
    let deletions_directory = TempDir::new_in(".").unwrap().into_path();
    let deletions = FasterKv::new_auctions_store(
        1 << 24,
        1 * 1024 * 1024 * 1024,
        deletions_directory.to_str().unwrap().to_string(),
    )
    .unwrap();
    let mut deletions_store_serial = 0;
    let accumulations_directory = TempDir::new_in(".").unwrap().into_path();
    let accumulations = FasterKv::new_u64_store(
        1 << 24,
        512 * 1024 * 1024,
        accumulations_directory.to_str().unwrap().to_string(),
    )
    .unwrap();
    let mut accumulations_store_serial = 0;
    let hot_items_directory = TempDir::new_in(".").unwrap().into_path();
    let hot_items = FasterKv::new_u64_pair_store(
        1 << 24,
        512 * 1024 * 1024,
        hot_items_directory.to_str().unwrap().to_string(),
    )
    .unwrap();
    let mut hot_items_store_serial = 0;

    input
        .bids(scope)
        .map(move |b| {
            (
                b.auction,
                Date::new(((*b.date_time / window_slide_ns) + 1) * window_slide_ns),
            )
        })
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Q5 Accumulate",
            None,
            move |input, output, notificator| {
                let mut bids_buffer = vec![];
                input.for_each(|time, data| {
                    data.swap(&mut bids_buffer);
                    let slide = Date::new(
                        ((*nt.to_nexmark_time(*time.time()) / window_slide_ns) + 1)
                            * window_slide_ns,
                    );
                    let downgrade = time.delayed(&nt.from_nexmark_time(slide));
                    notificator.notify_at(downgrade.clone());

                    // Collect all bids in a different slide.
                    for &(auction, a_time) in bids_buffer.iter() {
                        if a_time != slide {
                            additions.rmw_auction(
                                nt.from_nexmark_time(a_time) as u64,
                                auction as u64,
                                additions_store_serial,
                            );
                            maybe_refresh_faster(&additions, &mut additions_store_serial);
                            notificator.notify_at(time.delayed(&nt.from_nexmark_time(a_time)));
                        }
                    }
                    bids_buffer.retain(|&(_, a_time)| a_time == slide);

                    // Collect all bids in the same slide.
                    let bids = bids_buffer.drain(..).map(|(b, _)| b as u64).collect();
                    additions.rmw_auctions(
                        nt.from_nexmark_time(slide) as u64,
                        bids,
                        additions_store_serial,
                    );
                    maybe_refresh_faster(&additions, &mut additions_store_serial);
                });

                notificator.for_each(|time, _, notificator| {
                    let (status, recv) =
                        additions.read_auctions(*time.time() as u64, additions_store_serial);
                    if status == status::PENDING {
                        additions.complete_pending(true);
                    }
                    maybe_refresh_faster(&additions, &mut additions_store_serial);
                    if let Ok(additions) = recv.recv() {
                        for auction in additions.iter() {
                            accumulations.rmw_u64(*auction, 1, accumulations_store_serial);
                            maybe_refresh_faster(&accumulations, &mut accumulations_store_serial);
                        }
                        let new_time = time.time() + (window_slice_count * window_slide_ns);
                        let mut copied_additions = Vec::with_capacity(additions.len());
                        copied_additions.extend_from_slice(additions);
                        deletions.upsert_auctions(
                            new_time as u64,
                            copied_additions,
                            deletions_store_serial,
                        );
                        maybe_refresh_faster(&deletions, &mut deletions_store_serial);
                        notificator.notify_at(time.delayed(&new_time));
                    }
                    let (status, recv) =
                        deletions.read_auctions(*time.time() as u64, deletions_store_serial);
                    if status == status::PENDING {
                        deletions.complete_pending(true);
                    }
                    maybe_refresh_faster(&deletions, &mut deletions_store_serial);
                    if let Ok(deletions) = recv.recv() {
                        for auction in deletions {
                            let (status, recv) =
                                accumulations.read_u64(*auction, accumulations_store_serial);
                            if status == status::PENDING {
                                accumulations.complete_pending(true);
                            }
                            maybe_refresh_faster(&accumulations, &mut accumulations_store_serial);
                            match recv.recv() {
                                Ok(entry) => {
                                    if entry == 1 {
                                        accumulations
                                            .delete_u64(*auction, accumulations_store_serial);
                                    } else {
                                        accumulations.rmw_decrease_u64(
                                            *auction,
                                            1,
                                            accumulations_store_serial,
                                        );
                                    }
                                    maybe_refresh_faster(
                                        &accumulations,
                                        &mut accumulations_store_serial,
                                    );
                                }
                                Err(_) => panic!("entry has to exist"),
                            }
                        }
                    }
                    let mut highest_count: Option<(u64, u64)> = None;
                    let iterator = accumulations.get_iterator_u64();
                    while let Some(record) = iterator.get_next() {
                        let auction = record.key.unwrap();
                        let count = record.value.unwrap();
                        if highest_count.is_none() || count > highest_count.unwrap().0 {
                            highest_count = Some((count, auction));
                        }
                    }
                    if let Some((count, auction)) = highest_count {
                        output.session(&time).give((auction as usize, count));
                    }
                })
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
                    maybe_refresh_faster(&hot_items, &mut hot_items_store_serial);
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
                    maybe_refresh_faster(&hot_items, &mut hot_items_store_serial);
                    notificator.notify_at(time.delayed(&time.time()))
                });

                notificator.for_each(|cap, _, _| {
                    let (status, recv) =
                        hot_items.read_u64_pair(*cap.time() as u64, hot_items_store_serial);
                    if status == status::PENDING {
                        hot_items.complete_pending(true);
                    }
                    maybe_refresh_faster(&hot_items, &mut hot_items_store_serial);
                    output.session(&cap).give(*recv.recv().unwrap().0 as usize);
                });
            },
        )
}
