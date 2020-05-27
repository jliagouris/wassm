use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use crate::event::Date;

use crate::queries::{NexmarkInput, NexmarkTimer};

pub fn q7_managed<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_size_ns: usize,
) -> Stream<S, usize> {
    input
        .bids(scope)
        .map(move |b| {
            (
                Date::new(((*b.date_time / window_size_ns) + 1) * window_size_ns),
                b.price,
            )
        })
        .unary_notify(
            Pipeline,
            "Q7 Pre-Reduce",
            None,
            move |input, output, notificator, state_handle| {
                let mut pre_reduce_state = state_handle.get_managed_map("pre-reduce");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at end of epoch
                    notificator.notify_at(
                        time.delayed(&(((time.time() / window_size_ns) + 1) * window_size_ns)),
                    );
                    data.swap(&mut buffer);
                    for &(b_time, b_price) in buffer.iter() {
                        let epoch = nt.from_nexmark_time(b_time);
                        let current_highest = pre_reduce_state.get(&epoch).map_or(0, |v| *v);
                        if b_price > current_highest {
                            pre_reduce_state.insert(epoch, b_price);
                        }
                    }
                });

                notificator.for_each(|cap, _, _| {
                    if let Some(max_price) = pre_reduce_state.remove(&cap.time()) {
                        output.session(&cap).give((*cap.time(), max_price));
                    }
                });
            },
        )
        .unary_notify(
            Exchange::new(move |x: &(usize, usize)| (x.0 / window_size_ns) as u64),
            "Q7 All-Reduce",
            None,
            move |input, output, notificator, state_handle| {
                let mut all_reduce_state = state_handle.get_managed_map("all-reduce");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at end of epoch
                    notificator.notify_at(time.retain());
                    data.swap(&mut buffer);
                    for &(b_time, b_price) in buffer.iter() {
                        let current_highest = all_reduce_state.get(&b_time).map_or(0, |v| *v);
                        if b_price > current_highest {
                            all_reduce_state.insert(b_time, b_price);
                        }
                    }
                });

                notificator.for_each(|cap, _, _| {
                    if let Some(max_price) = all_reduce_state.remove(&cap.time()) {
                        output.session(&cap).give(max_price);
                    }
                });
            },
        )
}
