use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};

pub fn q8_managed_map<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_size_ns: usize,
) -> Stream<S, usize> {
    let auctions = input.auctions(scope).map(|a| (a.seller, a.date_time));

    let people = input.people(scope).map(|p| (p.id, p.date_time));

    people.binary_notify(
        &auctions,
        Exchange::new(|p: &(usize, _)| p.0 as u64),
        Exchange::new(|a: &(usize, _)| a.0 as u64),
        "Q8 join",
        None,
        move |input1, input2, output, notificator, state_handle| {
            let mut new_people = state_handle.get_managed_map("new_people");
            let mut auctions_state = state_handle.get_managed_map("auctions");
            let mut index_state = state_handle.get_managed_value("entries");

            // Notice new people.
            input1.for_each(|time, data| {
                notificator.notify_at(time.retain());
                for (person, p_time) in data.iter().cloned() {
                    new_people.insert(person, p_time);
                }
            });

            // Notice new auctions.
            input2.for_each(|time, data| {
                let ts = *time.time();
                let mut data_vec = vec![];
                data.swap(&mut data_vec);
                let mut stored_auctions = auctions_state.remove(&ts).unwrap_or(Vec::new());
                stored_auctions.extend(data_vec);
                auctions_state.insert(ts, stored_auctions);
                notificator.notify_at(time.retain());
            });

            notificator.for_each(|cap, _, _| {
                let capability_time = *cap.time();
                let mut entries_to_check = index_state.take().unwrap_or(Vec::new());
                entries_to_check.push(capability_time);
                let mut to_keep = Vec::new();
                for ts in entries_to_check { // ts <= capability_time
                    if let Some(mut auctions) = auctions_state.remove(&ts) {
                        let mut session = output.session(&cap);
                        for &(person, time) in auctions.iter() {
                            if time <= nt.to_nexmark_time(capability_time) {
                                if let Some(p_time) = new_people.get(&person) {
                                    if *time < **p_time + window_size_ns {
                                            session.give(person);
                                    }
                                }
                            }
                        }
                        auctions.retain(|&(_, time)| time > nt.to_nexmark_time(capability_time));
                        if auctions.len() > 0 {
                            // Put it back in state
                            auctions_state.insert(ts, auctions);
                            to_keep.push(ts)
                        }
                    } 
                }
                // Update entries
                index_state.set(to_keep);
            });
        },
    )
}
