use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid};

use super::maybe_refresh_faster;
use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::Auction as CAuction;
use faster_rs::Bid as CBid;
use faster_rs::{status, FasterKv};
use tempfile::TempDir;

fn is_valid_bid(bid: &Bid, auction: CAuction) -> bool {
    bid.price >= auction.reserve
        && auction.date_time <= *bid.date_time
        && *bid.date_time < auction.expires
}

pub fn q4_q6_common<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (usize, (usize, usize))> {
    let bids = input.bids(scope);
    let auctions = input.auctions(scope);

    let state_dir = TempDir::new_in(".").expect("Unable to create FASTER directory");
    let state = FasterKv::new_auction_bids_store(
        1 << 24,
        6 * 1024 * 1024 * 1024,
        state_dir.into_path().to_str().unwrap().to_string(),
    )
    .expect("Couldn't initialise FASTER");
    state.start_session();
    let mut state_serial = 0;

    let expirations_dir = TempDir::new_in(".").expect("Unable to create FASTER directory");
    let expirations = FasterKv::new_auctions_store(
        1 << 24,
        1536 * 1024 * 1024,
        expirations_dir.into_path().to_str().unwrap().to_string(),
    )
    .expect("Couldn't initialise FASTER");
    expirations.start_session();
    let mut expirations_serial = 0;

    bids.binary_notify(
        &auctions,
        Exchange::new(|b: &Bid| b.auction as u64),
        Exchange::new(|a: &Auction| a.id as u64),
        "Q4 Auction close",
        None,
        move |input1, input2, output, notificator| {
            // Record each bid.
            input1.for_each(|time, data| {
                for bid in data.iter().cloned() {
                    let (res, recv) = state.read_auction_bids(bid.auction as u64, state_serial);
                    if res == status::PENDING {
                        state.complete_pending(true);
                    }
                    maybe_refresh_faster(&state, &mut state_serial);
                    match recv.recv() {
                        Ok(entry) => {
                            if let Some(auction) = entry.0 {
                                if is_valid_bid(&bid, auction) {
                                    // bid must fall between auction creation and expiration
                                    if let Some(existing) = entry.1.get(0) {
                                        if existing.price < bid.price {
                                            entry.1[0] = CBid {
                                                price: bid.price,
                                                date_time: *bid.date_time,
                                                bidder: bid.bidder,
                                            };
                                        }
                                    } else {
                                        state.rmw_auction_bids_bid(
                                            bid.auction as u64,
                                            *bid.date_time,
                                            bid.price,
                                            bid.bidder,
                                            state_serial,
                                        );
                                        maybe_refresh_faster(&state, &mut state_serial);
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            state.rmw_auction_bids_bid(
                                bid.auction as u64,
                                *bid.date_time,
                                bid.price,
                                bid.bidder,
                                state_serial,
                            );
                            maybe_refresh_faster(&state, &mut state_serial);
                        }
                    }
                }
            });

            // Record each auction.
            input2.for_each(|time, data| {
                for auction in data.iter().cloned() {
                    notificator.notify_at(time.delayed(&nt.from_nexmark_time(auction.expires)));
                    expirations.rmw_auction(
                        nt.from_nexmark_time(auction.expires) as u64,
                        auction.id as u64,
                        expirations_serial,
                    );
                    maybe_refresh_faster(&expirations, &mut expirations_serial);
                    state.rmw_auction_bids_auction(
                        auction.id as u64,
                        auction.id,
                        auction.category,
                        *auction.date_time,
                        *auction.expires,
                        auction.reserve,
                        state_serial,
                    );
                    maybe_refresh_faster(&state, &mut state_serial);
                    let (res, recv) = state.read_auction_bids(auction.id as u64, state_serial);
                    if res == status::PENDING {
                        state.complete_pending(true);
                    }
                    maybe_refresh_faster(&state, &mut state_serial);
                    let (_, bids) = recv.recv().unwrap();
                    if let Some(bid) = bids.iter().max_by_key(|bid| bid.price) {
                        bids[0] = CBid {
                            price: bid.price,
                            date_time: bid.date_time,
                            bidder: bid.bidder,
                        }
                    }
                }
            });

            notificator.for_each(|cap, _, _| {
                let mut session = output.session(&cap);
                let (res, recv) = expirations.read_auctions(*cap.time() as u64, expirations_serial);
                if res == status::PENDING {
                    expirations.complete_pending(true);
                }
                maybe_refresh_faster(&expirations, &mut expirations_serial);
                for auction_id in recv.recv().unwrap() {
                    let (res, recv) = state.read_auction_bids(*auction_id, state_serial);
                    if res == status::PENDING {
                        state.complete_pending(true);
                    }
                    maybe_refresh_faster(&state, &mut state_serial);
                    let delete = match recv.recv() {
                        Err(_) => false,
                        Ok((auction, bids)) => match auction {
                            None => {
                                let mut vec = Vec::with_capacity(bids.len());
                                vec.copy_from_slice(bids);
                                vec.retain(|bid| bid.date_time > *cap.time());
                                bids.is_empty()
                            }
                            Some(auction) => {
                                if auction.expires == *cap.time() {
                                    if bids.len() > 0 {
                                        session.give((
                                            auction.category,
                                            (bids[0].bidder, bids[0].price),
                                        ));
                                    }
                                    true
                                } else {
                                    false
                                }
                            }
                        },
                    };
                    if delete {
                        state.delete_auction_bids(*auction_id, state_serial);
                        maybe_refresh_faster(&state, &mut state_serial);
                    }
                }
                expirations.delete_auctions(*cap.time() as u64, expirations_serial);
                maybe_refresh_faster(&expirations, &mut expirations_serial);
            });
        },
    )
}
