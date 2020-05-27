use faster_rs::{status, FasterKv};
use std::collections::HashMap;
use tempfile::TempDir;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Filter, Operator};
use timely::dataflow::{Scope, Stream};

use super::maybe_refresh_faster;
use crate::event::{Auction, Person};

use crate::queries::{NexmarkInput, NexmarkTimer};
use std::ffi::CStr;
use std::sync::mpsc::RecvTimeoutError;

pub fn q3<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (String, String, String, usize)> {
    let auctions = input.auctions(scope).filter(|a| a.category == 10);

    let people = input
        .people(scope)
        .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

    let mut auctions_buffer = vec![];
    let mut people_buffer = vec![];

    let people_dir = TempDir::new_in(".").expect("Unable to create FASTER directory");
    let people_store = FasterKv::new_person_store(
        1 << 24,
        1 * 1024 * 1024 * 1024,
        people_dir.into_path().to_str().unwrap().to_string(),
    )
    .expect("Couldn't initialise FASTER");
    people_store.start_session();
    let mut people_store_serial = 0;

    let auctions_dir = TempDir::new_in(".").expect("Unable to create FASTER directory");
    let auctions_store = FasterKv::new_auctions_store(
        1 << 24,
        7 * 1024 * 1024 * 1024,
        auctions_dir.into_path().to_str().unwrap().to_string(),
    )
    .expect("Couldn't initialise FASTER");
    auctions_store.start_session();
    let mut auctions_store_serial = 0;

    auctions.binary(
        &people,
        Exchange::new(|a: &Auction| a.seller as u64 / 100),
        Exchange::new(|p: &Person| p.id as u64 / 100),
        "Q3 Join",
        |_capability, _info| {
            move |input1, input2, output| {
                // Process each input auction.
                input1.for_each(|time, data| {
                    data.swap(&mut auctions_buffer);
                    let mut session = output.session(&time);
                    for auction in auctions_buffer.drain(..) {
                        let (read_status, recv_person) =
                            people_store.read_person(auction.seller as u64, people_store_serial);
                        maybe_refresh_faster(&people_store, &mut people_store_serial);
                        if read_status == status::PENDING {
                            people_store.complete_pending(true);
                        }
                        if let Ok(person) = recv_person.recv() {
                            let name = String::from(
                                unsafe { CStr::from_ptr(person.name) }
                                    .to_str()
                                    .expect("Couldn't read name"),
                            );
                            let city = String::from(
                                unsafe { CStr::from_ptr(person.city) }
                                    .to_str()
                                    .expect("Couldn't read city"),
                            );
                            let state = String::from(
                                unsafe { CStr::from_ptr(person.state) }
                                    .to_str()
                                    .expect("Couldn't read state"),
                            );
                            session.give((name, city, state, auction.id));
                        }
                        auctions_store.rmw_auction(
                            auction.seller as u64,
                            auction.id as u64,
                            auctions_store_serial,
                        );
                        maybe_refresh_faster(&auctions_store, &mut auctions_store_serial);
                    }
                });

                // Process each input person.
                input2.for_each(|time, data| {
                    data.swap(&mut people_buffer);
                    let mut session = output.session(&time);
                    for person in people_buffer.drain(..) {
                        let (read_status, recv_auctions) =
                            auctions_store.read_auctions(person.id as u64, auctions_store_serial);
                        maybe_refresh_faster(&auctions_store, &mut auctions_store_serial);
                        if read_status == status::PENDING {
                            auctions_store.complete_pending(true);
                        }
                        if let Ok(auctions) = recv_auctions.recv() {
                            for auction in auctions.iter() {
                                session.give((
                                    person.name.clone(),
                                    person.city.clone(),
                                    person.state.clone(),
                                    *auction as usize,
                                ));
                            }
                        }
                        people_store.upsert_person(
                            person.id as u64,
                            &person.name,
                            &person.city,
                            &person.state,
                            people_store_serial,
                        );
                        maybe_refresh_faster(&people_store, &mut people_store_serial);
                    }
                });
            }
        },
    )
}
