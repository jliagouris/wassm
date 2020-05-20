use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::{Scope, Stream};
use timely::state::primitives::ManagedMap;

use crate::event::{Auction, Bid};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;

fn is_valid_bid(bid: &Bid, auction: &Auction) -> bool {
    bid.price >= auction.reserve
        && auction.date_time <= bid.date_time
        && bid.date_time < auction.expires
}

#[derive(Serialize, Deserialize)]
struct AuctionBids(Option<Auction>, Vec<Bid>);

impl FasterRmw for AuctionBids {
    fn rmw(&self, _modification: Self) -> Self {
        unimplemented!()
    }
}


pub fn q4_q6_common_managed<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
) -> Stream<S, (Auction, Bid)> {
    let bids = input.bids(scope);
    let auctions = input.auctions(scope);

    bids.binary_notify(
        &auctions,
        Exchange::new(|b: &Bid| b.auction as u64),
        Exchange::new(|a: &Auction| a.id as u64),
        "Q4 Auction close",
        None,
        move |input1, input2, output, notificator, state_handle| {
            let mut state: Box<ManagedMap<usize, AuctionBids>> = state_handle.get_managed_map("state");
            let mut expirations: Box<ManagedMap<usize, Vec<Auction>>> = state_handle.get_managed_map("expirations");
            // Record each bid.
            // NB: We don't summarize as the max, because we don't know which are valid.
            input1.for_each(|time, data| {
                for bid in data.iter().cloned() {
                    let auction_bids = state.remove(&bid.auction);
                    match auction_bids {
                        Some(mut entry) => {
                            let auction_id = bid.auction;
                            if let Some(auction) = entry.0.clone() {
                                if is_valid_bid(&bid, &auction) {
                                    // bid must fall between auction creation and expiration
                                    if let Some(existing) = entry.1.get(0) {
                                        if existing.price < bid.price {
                                            entry.1[0] = bid;
                                        }
                                    } else {
                                        entry.1.push(bid);
                                    }
                                }
                            }
                            state.insert(auction_id, entry);
                        }
                        None => {
                            state.insert(bid.auction, AuctionBids(None, vec![bid]));
                        }
                    }
                }
            });

            // Record each auction.
            input2.for_each(|time, data| {
                for auction in data.iter().cloned() {
                    notificator.notify_at(time.delayed(&nt.from_nexmark_time(auction.expires)));
                    let auction_id = auction.id;
                    expirations.rmw(nt.from_nexmark_time(auction.expires), vec![auction.clone()]);
                    let mut auction_bids = state.remove(&auction_id).unwrap_or(AuctionBids(None, Vec::new()));
                    auction_bids.0 = Some(auction);
                    if let Some(bid) = auction_bids.1.iter().max_by_key(|bid| bid.price).cloned() {
                        auction_bids.1[0] = bid;
                    }
                    state.insert(auction_id, auction_bids);
                }
            });

            notificator.for_each(|cap, _, _| {
                let mut session = output.session(&cap);
                for auction in expirations.remove(cap.time()).expect("Must exist") {
                    let auction_bids = state.remove(&auction.id);
                    if let Some(mut auction_bids) = auction_bids {
                        let insert = match auction_bids.0 {
                            None => {
                                auction_bids.1.retain(|bid| bid.date_time > nt.to_nexmark_time(*cap.time()));
                                !auction_bids.1.is_empty()
                            },
                            Some(ref auction) => {
                                if auction.expires == nt.to_nexmark_time(*cap.time()) {
                                    if auction_bids.1.len() > 0 {
                                        session.give(
                                            (auction.clone(), auction_bids.1[0].clone()),
                                        );
                                    }
                                    false
                                } else {
                                    true
                                }
                            },
                        };
                        if insert {
                            state.insert(auction.id, auction_bids);
                        }
                    }
                }
            });
        },
    )
}
