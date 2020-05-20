use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Capability, Operator};
use timely::dataflow::{Scope, Stream};
use std::collections::HashMap;

use crate::queries::{NexmarkInput, NexmarkTimer};

pub fn q8_managed<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_size_ns: usize,
) -> Stream<S, usize> {
    let auctions = input.auctions(scope).map(|a| (a.seller, a.date_time));

    let people = input.people(scope).map(|p| (p.id, p.date_time));

    // Used for producing output
    let mut capabilities: HashMap<usize, Capability<usize>> = HashMap::new();

    people.binary_frontier(
        &auctions,
        Exchange::new(|p: &(usize, _)| p.0 as u64),
        Exchange::new(|a: &(usize, _)| a.0 as u64),
        "Q8 join",
        |_capability, _info, state_handle| {
            let mut new_people = state_handle.get_managed_map("new_people");
            let mut auctions_state = state_handle.get_managed_value("auctions");

            move |input1, input2, output| {
                // Notice new people.
                input1.for_each(|_time, data| {
                    for (person, p_time) in data.iter().cloned() {
                        new_people.insert(person, p_time);
                    }
                });

                // Notice new auctions.
                input2.for_each(|time, data| {
                    let ts = *time.time();
                    let _ = capabilities.entry(ts).or_insert_with(|| time.retain());
                    let mut data_vec = vec![];
                    data.swap(&mut data_vec);
                    let mut stored_auctions = auctions_state.take().unwrap_or(Vec::new());
                    stored_auctions.push((ts, data_vec));
                    auctions_state.set(stored_auctions);
                    //auctions_state.rmw(vec![(*time.time(), data_vec)]);
                    //notificator.notify_at(time.delayed(time.time()));
                });

                // Determine least timestamp we might still see.
                let complete1 = input1
                    .frontier
                    .frontier()
                    .get(0)
                    .cloned()
                    .unwrap_or(usize::max_value());
                let complete2 = input2
                    .frontier
                    .frontier()
                    .get(0)
                    .cloned()
                    .unwrap_or(usize::max_value());
                let complete = std::cmp::min(complete1, complete2);

                //notificator.for_each(|cap, _, _| {
                let mut auctions_vec = auctions_state.take().unwrap_or(Vec::new());
        		let mut caps_to_remove = Vec::new();
                for (capability_time, auctions) in auctions_vec.iter_mut() {
                    // If seller's record corresponds to a closed epoch
                    if *capability_time < complete {
                        caps_to_remove.push(*capability_time);
                        // println!("Capability: {}",*capability_time);
                        let cap = capabilities.get_mut(capability_time).expect("Capability must exist.");
                        let mut session = output.session(&cap);
                        for &(person, time) in auctions.iter() {
                            // If person's record corresponds to a closed epoch
                            if time < nt.to_nexmark_time(complete) {
                                if let Some(p_time) = new_people.get(&person) {
                                    // Do the join within the last 12 hours
                                    if *time < **p_time + window_size_ns {
                                        // seller's time - person's time is within the 12 hours range
                                        session.give(person);
                                    }
                                }
                            }
                        }
                        auctions.retain(|&(_, time)| time >= nt.to_nexmark_time(complete));
                        if let Some(minimum) = auctions.iter().map(|x| x.1).min() {
                            cap.downgrade(&nt.from_nexmark_time(minimum));
                        }
                    }
                }
        		for cap in caps_to_remove.drain(..) {
        			capabilities.remove(&cap).expect("Cap to remove must exist");
        		}
                auctions_vec.retain(|&(_, ref list)| !list.is_empty());
                auctions_state.set(auctions_vec);
                //});
            }
        },
    )
}
