use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

// 2nd window implementation using merge
pub fn window_2b_rocksdb_rank<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize, usize)> {

    let mut max_window_seen = 0;

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
                    // The end timestamp of the slide the current epoch corresponds to
                    let slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    if max_window_seen < slide {
                        for window_start in (max_window_seen..slide).step_by(window_slide_ns) {
                            // Merging in RocksDB needs a first 'put' operation to work properly
                            // println!("First PUT operation for window start: {:?}", window_start);
                            window_buckets.insert(window_start, vec![]);  // Initialize window state
                        }
                        max_window_seen = slide;
                    }
                    for record in buffer.iter() {
                        let windows = assign_windows(record.1, window_slide_ns, window_size);
                        for win in windows {
                            // Notify at end of this window
                            notificator.notify_at(time.delayed(&(win + window_size)));
                            // println!("Asking notification for end of window: {:?}", win + window_size);
                            window_buckets.rmw(win, vec![*record]);
                            // println!("Appending record with timestamp {} and auction id {} to window with start timestamp {}.", record.1, record.0, win);
                        }
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // println!("Firing and cleaning window with start timestamp {}.", cap.time() - window_size);
                    let start_timestamp = cap.time() - window_size;
                    let mut records = window_buckets.remove(&start_timestamp).expect("Must exist");
                    // Apply the rank function to the window
                    records.sort_unstable_by(|a, b| a.0.cmp(&b.0)); // Sort auctions by id
                    let mut rank = 1;
                    let mut count = 0;
                    let mut current_record = records[0];
                    for record in records {
                        // output (timestamp, auctionID, rank)
                        let auction = record;
                        if auction.0 != current_record.0 {
                            // increase rank and update current
                            rank += count;
                            count = 0;
                            current_record = auction;
                        }
                        count += 1;
                        output.session(&cap).give((*cap.time(), auction.0, rank));
                        // println!("*** End of window: {:?}, Auction: {:?}, Rank: {:?}", cap.time(), auction.0, rank);
                    }
                });
            },
        )
}
