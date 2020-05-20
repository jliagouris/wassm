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

// 2nd window implementation using merge
pub fn window_2b_rocksdb_count<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize)> {

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
                            window_buckets.insert(window_start, 0);  // Initialize window state
                        }
                        max_window_seen = slide;
                    }
                    for record in buffer.iter() {
                        let windows = assign_windows(record.1, window_slide_ns, window_size);
                        for win in windows {
                            // Notify at end of this window
                            notificator.notify_at(time.delayed(&(win + window_size)));
                            // println!("Asking notification for end of window: {:?}", win + window_size);
                            window_buckets.rmw(win, 1);
                            // println!("Appending record with timestamp {} to window with start timestamp {}.", record.1, win);
                        }
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // println!("Firing and cleaning window with start timestamp {}.", cap.time() - window_size);
                    let start_timestamp = cap.time() - window_size;
                    let count = window_buckets.remove(&start_timestamp).expect("Must exist");
                    // println!("*** Window start: {}, count {}.", cap.time() - window_size, count);
                    output.session(&cap).give((*cap.time(), count));
                });
            },
        )
}
