use std::rc::Rc;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid, Date, Person};

pub mod nexmark;
pub mod windows;

use faster_rs::FasterKv;

pub fn assign_windows(event_time: usize,
                      window_slide: usize,
                      window_size: usize
                     ) -> Vec<usize> {
    let mut windows = Vec::new();
    let last_window_start = event_time - (event_time + window_slide) % window_slide;
    let num_windows = (window_size as f64 / window_slide as f64).ceil() as i64;
    for i in 0i64..num_windows {
        let w_id = last_window_start as i64 - (i * window_slide as i64);
        if w_id >= 0 && (event_time < w_id  as usize + window_size) {
            windows.push(w_id as usize);
        }
    }
    windows
}

#[inline(always)]
fn maybe_refresh_faster(faster: &FasterKv, monotonic_serial_number: &mut u64) {
    if *monotonic_serial_number % (1 << 4) == 0 {
        faster.refresh();
        if *monotonic_serial_number % (1 << 10) == 0 {
            faster.complete_pending(true);
        }
    }
    if *monotonic_serial_number % (1 << 20) == 0 {
        println!("Size: {}", faster.size());
    }
    *monotonic_serial_number += 1;
}


pub struct NexmarkInput<'a> {
    pub bids: &'a Rc<EventLink<usize, Bid>>,
    pub auctions: &'a Rc<EventLink<usize, Auction>>,
    pub people: &'a Rc<EventLink<usize, Person>>,
    pub closed_auctions: &'a Rc<EventLink<usize, (Auction, Bid)>>,
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
    ) -> Stream<S, (Auction, Bid)> {
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
