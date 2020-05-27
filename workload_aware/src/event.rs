use faster_rs::FasterRmw;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::Rng;
use std::cmp::{max, min};

use crate::config::NEXMarkConfig;

trait NEXMarkRng {
    fn gen_string(&mut self, _max: usize) -> String;
    fn gen_price(&mut self) -> usize;
}

impl NEXMarkRng for SmallRng {
    fn gen_string(&mut self, max: usize) -> String {
        String::new()
    }

    fn gen_price(&mut self) -> usize {
        (10.0_f32.powf(self.gen::<f32>() * 6.0) * 100.0).round() as usize
    }
}

type Id = usize;
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Clone,
    Serialize,
    Deserialize,
    Debug,
    Abomonation,
    Hash,
    Copy,
    Default,
)]
pub struct Date(usize);

impl Date {
    pub fn new(date_time: usize) -> Date {
        Date(date_time)
    }
}

impl ::std::ops::Deref for Date {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl ::std::ops::Add for Date {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Date(self.0 + other.0)
    }
}
impl ::std::ops::Sub for Date {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Date(self.0 - other.0)
    }
}

impl FasterRmw for Date {
    fn rmw(&self, modification: Self) -> Self {
        Date(self.0 + modification.0)
    }
}

const MIN_STRING_LENGTH: usize = 3;

#[derive(Serialize, Deserialize, Abomonation, Debug)]
struct EventCarrier {
    time: Date,
    event: Event,
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug, Abomonation)]
#[serde(tag = "type")]
pub enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}

impl Event {
    pub fn time(&self) -> Date {
        match *self {
            Event::Person(ref p) => p.date_time,
            Event::Auction(ref a) => a.date_time,
            Event::Bid(ref b) => b.date_time,
        }
    }

    pub fn create(events_so_far: usize, rng: &mut SmallRng, nex: &mut NEXMarkConfig) -> Self {
        let rem = nex.next_adjusted_event(events_so_far) % nex.proportion_denominator;
        let timestamp = Date(nex.event_timestamp_ns(nex.next_adjusted_event(events_so_far)));
        let id = nex.first_event_id + nex.next_adjusted_event(events_so_far);

        if rem < nex.person_proportion {
            Event::Person(Person::new(id, timestamp, rng, nex))
        } else if rem < nex.person_proportion + nex.auction_proportion {
            Event::Auction(Auction::new(events_so_far, id, timestamp, rng, nex))
        } else {
            Event::Bid(Bid::new(id, timestamp, rng, nex))
        }
    }

    pub fn id(&self) -> Id {
        match *self {
            Event::Person(ref p) => p.id,
            Event::Auction(ref a) => a.id,
            Event::Bid(ref b) => b.auction, // Bid eventss don't have ids, so use the associated auction id
        }
    }

}

#[derive(
    Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash,
)]
pub struct Person {
    pub id: Id,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: Date,
}

impl FasterRmw for Person {
    fn rmw(&self, _modification: Person) -> Person {
        unimplemented!();
    }
}

impl Person {
    pub fn from(event: Event) -> Option<Person> {
        match event {
            Event::Person(p) => Some(p),
            _ => None,
        }
    }

    fn new(id: usize, time: Date, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Self {
        Person {
            id: Self::last_id(id, nex) + nex.first_person_id,
            name: String::new(),
            email_address: String::new(),
            credit_card: String::new(),
            city: nex.us_cities.choose(rng).unwrap().clone(),
            state: nex.us_states.choose(rng).unwrap().clone(),
            date_time: time,
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Id {
        let people = Self::last_id(id, nex) + 1;
        let active = min(people, nex.active_people);
        people - active + rng.gen_range(0, active + nex.person_id_lead)
    }

    fn last_id(id: usize, nex: &NEXMarkConfig) -> Id {
        let epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if nex.person_proportion <= offset {
            offset = nex.person_proportion - 1;
        }
        epoch * nex.person_proportion + offset
    }
}

#[derive(
    Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash,
)]
pub struct Auction {
    pub id: Id,
    pub item_name: String,
    pub description: String,
    pub initial_bid: usize,
    pub reserve: usize,
    pub date_time: Date,
    pub expires: Date,
    pub seller: Id,
    pub category: Id,
}

impl Auction {
    pub fn from(event: Event) -> Option<Auction> {
        match event {
            Event::Auction(p) => Some(p),
            _ => None,
        }
    }

    fn new(
        events_so_far: usize,
        id: usize,
        time: Date,
        rng: &mut SmallRng,
        nex: &NEXMarkConfig,
    ) -> Self {
        let initial_bid = rng.gen_price();
        let seller = if rng.gen_range(0, nex.hot_seller_ratio) > 0 {
            (Person::last_id(id, nex) / nex.hot_seller_ratio_2) * nex.hot_seller_ratio_2
        } else {
            Person::next_id(id, rng, nex)
        };
        Auction {
            id: Self::last_id(id, nex) + nex.first_auction_id,
            item_name: rng.gen_string(20),
            description: rng.gen_string(100),
            initial_bid: initial_bid,
            reserve: initial_bid + rng.gen_price(),
            date_time: time,
            expires: time + Self::next_length(events_so_far, rng, time, nex),
            seller: seller + nex.first_person_id,
            category: nex.first_category_id + rng.gen_range(0, nex.num_categories),
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Id {
        let max_auction = Self::last_id(id, nex);
        let min_auction = if max_auction < nex.in_flight_auctions {
            0
        } else {
            max_auction - nex.in_flight_auctions
        };
        min_auction + rng.gen_range(0, max_auction - min_auction + 1 + nex.auction_id_lead)
    }

    fn last_id(id: usize, nex: &NEXMarkConfig) -> Id {
        let mut epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if offset < nex.person_proportion {
            epoch -= 1;
            offset = nex.auction_proportion - 1;
        } else if nex.person_proportion + nex.auction_proportion <= offset {
            offset = nex.auction_proportion - 1;
        } else {
            offset -= nex.person_proportion;
        }
        epoch * nex.auction_proportion + offset
    }

    fn next_length(
        events_so_far: usize,
        rng: &mut SmallRng,
        time: Date,
        nex: &NEXMarkConfig,
    ) -> Date {
        let current_event = nex.next_adjusted_event(events_so_far);
        let events_for_auctions =
            (nex.in_flight_auctions * nex.proportion_denominator) / nex.auction_proportion;
        let future_auction = nex.event_timestamp_ns(current_event + events_for_auctions);

        let horizon = future_auction - time.0;
        Date(1 + rng.gen_range(0, max(horizon * 2, 1)))
    }
}

#[derive(
    Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash,
)]
pub struct Bid {
    pub auction: Id,
    pub bidder: Id,
    pub price: usize,
    pub date_time: Date,
}

impl Bid {
    pub fn from(event: Event) -> Option<Bid> {
        match event {
            Event::Bid(p) => Some(p),
            _ => None,
        }
    }

    fn new(id: usize, time: Date, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Self {
        let auction = if 0 < rng.gen_range(0, nex.hot_auction_ratio) {
            (Auction::last_id(id, nex) / nex.hot_auction_ratio_2) * nex.hot_auction_ratio_2
        } else {
            Auction::next_id(id, rng, nex)
        };
        let bidder = if 0 < rng.gen_range(0, nex.hot_bidder_ratio) {
            (Person::last_id(id, nex) / nex.hot_bidder_ratio_2) * nex.hot_bidder_ratio_2 + 1
        } else {
            Person::next_id(id, rng, nex)
        };
        Bid {
            auction: auction + nex.first_auction_id,
            bidder: bidder + nex.first_person_id,
            price: rng.gen_price(),
            date_time: time,
        }
    }
}
