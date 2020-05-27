use std::rc::Rc;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid, Date, Person};

mod q1;
mod q2;
mod q3;
mod q4;
mod q4_flex;
mod q4_q6_common;
mod q5;
mod q5_index;
mod q6;
mod q7;
mod q8;

pub use self::q1::q1;
pub use self::q2::q2;
pub use self::q3::q3;
pub use self::q4::q4;
pub use self::q4_flex::q4_flex;
pub use self::q4_q6_common::q4_q6_common;
pub use self::q5::q5;
pub use self::q5_index::q5_index;
pub use self::q6::q6;
pub use self::q7::q7;
pub use self::q8::q8;
use faster_rs::FasterKv;

#[inline(always)]
fn maybe_refresh_faster(faster: &FasterKv, monotonic_serial_number: &mut u64) {
    if *monotonic_serial_number % (1 << 4) == 0 {
        faster.refresh();
        if *monotonic_serial_number % (1 << 10) == 0 {
            faster.complete_pending(true);
        }
    }
    *monotonic_serial_number += 1;
}

pub struct NexmarkInput<'a> {
    pub bids: &'a Rc<EventLink<usize, Bid>>,
    pub auctions: &'a Rc<EventLink<usize, Auction>>,
    pub people: &'a Rc<EventLink<usize, Person>>,
    pub closed_auctions: &'a Rc<EventLink<usize, (usize, (usize, usize))>>,
    pub closed_auctions_flex: &'a Rc<EventLink<usize, (Auction, Bid)>>,
}

impl<'a> NexmarkInput<'a> {
    pub fn bids<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Bid> {
        Some(self.bids.clone()).replay_into(scope)
    }

    pub fn auctions<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Auction> {
        Some(self.auctions.clone()).replay_into(scope)
    }

    pub fn people<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Person> {
        Some(self.people.clone()).replay_into(scope)
    }

    pub fn closed_auctions<S: Scope<Timestamp = usize>>(
        &self,
        scope: &mut S,
    ) -> Stream<S, (usize, (usize, usize))> {
        Some(self.closed_auctions.clone()).replay_into(scope)
    }
}

#[derive(Copy, Clone)]
pub struct NexmarkTimer {
    pub time_dilation: usize,
}

impl NexmarkTimer {
    #[inline(always)]
    fn to_nexmark_time(self, x: usize) -> Date {
        debug_assert!(
            x.checked_mul(self.time_dilation).is_some(),
            "multiplication failed: {} * {}",
            x,
            self.time_dilation
        );
        Date::new(x * self.time_dilation)
    }

    #[inline(always)]
    fn from_nexmark_time(self, x: Date) -> usize {
        *x / self.time_dilation
    }
}
