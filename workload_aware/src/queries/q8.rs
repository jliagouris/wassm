use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use crate::event::Date;
use std::collections::HashMap;
use tempfile::TempDir;
use faster_rs::{FasterKv, status};

pub fn q8<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_size_ns: usize,
) -> Stream<S, usize> {
    let auctions = input.auctions(scope).map(move |a| (a.seller, nt.from_nexmark_time(a.date_time)));

    let people = input.people(scope).map(|p| (p.id, p.date_time));

    let new_people_directory = TempDir::new_in(".").unwrap().into_path();
    let new_people = FasterKv::new_u64_store(
        1 << 24,
        4 * 1024 * 1024 * 1024,
        new_people_directory.to_str().unwrap().to_string(),
    )
        .unwrap();
    let mut new_people_store_serial = 0;
    let auctions_state_directory = TempDir::new_in(".").unwrap().into_path();
    let auctions_state = FasterKv::new_u64_pairs_store(
        1 << 24,
        4 * 1024 * 1024 * 1024,
        auctions_state_directory.to_str().unwrap().to_string(),
    )
        .unwrap();
    let mut auctions_state_store_serial = 0;

    let mut index_state: Vec<usize> = Vec::new();
    people.binary_notify(
        &auctions,
        Exchange::new(|p: &(usize, _)| p.0 as u64),
        Exchange::new(|a: &(usize, _)| a.0 as u64),
        "Q8 join",
        None,
        move |input1, input2, output, notificator| {
            // Notice new people.
            input1.for_each(|time, data| {
                notificator.notify_at(time.retain());
                for (person, p_time) in data.iter().cloned() {
                    new_people.upsert_u64(person as u64, *p_time as u64, new_people_store_serial);
                    super::maybe_refresh_faster(&new_people, &mut new_people_store_serial);
                }
            });

            // Notice new auctions.
            input2.for_each(|time, data| {
                let ts = *time.time();
                let mut data_vec = vec![];
                data.swap(&mut data_vec);
                auctions_state.rmw_u64_pairs(ts as u64, data_vec, auctions_state_store_serial);
                super::maybe_refresh_faster(&auctions_state, &mut auctions_state_store_serial);
                notificator.notify_at(time.retain());
            });

            notificator.for_each(|cap, _, _| {
                let capability_time = *cap.time();
                let mut entries_to_check = index_state.clone();
                entries_to_check.push(capability_time);
                let mut to_keep = Vec::new();
                for ts in entries_to_check { // ts <= capability_time
                    let (res, recv) = auctions_state.read_u64_pairs(ts as u64, auctions_state_store_serial);
                    if res == status::PENDING {
                        auctions_state.complete_pending(true);
                    }
                    super::maybe_refresh_faster(&auctions_state, &mut auctions_state_store_serial);
                    if let Some(mut auctions) = recv.recv().ok() {
                        auctions_state.delete_u64_pairs(ts as u64, auctions_state_store_serial);
                        super::maybe_refresh_faster(&auctions_state, &mut auctions_state_store_serial);
                        let mut session = output.session(&cap);
                        for &(person, time) in auctions.iter() {
                            if time <= *nt.to_nexmark_time(capability_time) {
                                let (res, recv) = new_people.read_u64(person as u64, new_people_store_serial);
                                if res == status::PENDING {
                                    new_people.complete_pending(true);
                                }
                                super::maybe_refresh_faster(&new_people, &mut new_people_store_serial);
                                if let Some(p_time) = recv.recv().ok() {
                                    if time < *nt.to_nexmark_time(p_time as usize + window_size_ns) {
                                        session.give(person);
                                    }
                                }
                            }
                        }
                        auctions.retain(|&(_, time)| time > *nt.to_nexmark_time(capability_time));
                        if auctions.len() > 0 {
                            // Put it back in state
                            auctions_state.upsert_u64_pairs(ts as u64, auctions, auctions_state_store_serial);
                            super::maybe_refresh_faster(&auctions_state, &mut auctions_state_store_serial);
                            to_keep.push(ts)
                        }
                    }
                }
                // Update entries
                index_state = to_keep;
            });
        },
    )
}
