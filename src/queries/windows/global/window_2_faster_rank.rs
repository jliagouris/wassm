use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

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

pub fn window_2_faster_rank<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize, usize)> {
    input
        .bids(scope)
        .map(move |b| {
            (
                b.auction,
                *b.date_time
            )
        })
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                let window_size = window_slice_count * window_slide_ns;
                // window_start_timestamp -> window_contents
                let mut window_buckets = state_handle.get_managed_map("window_buckets");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let windows = assign_windows(record.1, window_slide_ns, window_size);
                        for win in windows {
                            // Notify at end of this window
                            notificator.notify_at(time.delayed(&(win + window_size)));
                            // println!("Asking notification for end of window: {:?}", win + window_size);
                            window_buckets.rmw(win, vec![*record]);
                            // println!("Appending record with timestamp {} to window with start timestamp {}.", record.1, win);
                        }
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // println!("Firing and cleaning window with start timestamp {}.", cap.time() - window_size);
                    let records = window_buckets.remove(&(cap.time() - window_size)).expect("Must exist");
                    let mut auctions = Vec::new();
                    for record in records.iter() {
                        auctions.push(record.0);
                    }
                    // println!("*** Window start: {}, contents {:?}.", cap.time() - window_size, records);
                    auctions.sort_unstable();
                    let mut rank = 1;
                    let mut count = 0;
                    let mut current_record = auctions[0];
                    for auction in &auctions {
                        // output (timestamp, auctionID, rank)
                        if *auction != current_record {
                            // increase rank and update current
                            rank+=count;
                            count = 0;
                            current_record = *auction;
                        }
                        count+=1;
                        output.session(&cap).give((*cap.time(), *auction, rank));
                        // println!("*** Start of window: {:?}, Auction: {:?}, Rank: {:?}", cap.time(), auction, rank);
                    }
                });
            },
        )
}
