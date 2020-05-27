//! NEXMark benchmark implementation.
//!
//! Query parameters based on http://datalab.cs.pdx.edu/niagara/NEXMark/ unless noted otherwise.

extern crate abomonation;
extern crate clap;
extern crate nexmark;
extern crate rand;
extern crate streaming_harness;
extern crate timely;

use std::alloc::System;

#[global_allocator]
static GLOBAL: System = System;

const TIME_DILATION: usize = 1;

use clap::{App, Arg};

use streaming_harness::util::ToNanos;

use timely::dataflow::operators::{Capture, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::ExchangeData;

use nexmark::event::Event;
use nexmark::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::inspect::Inspect;

#[allow(dead_code)]
fn verify<S: Scope, T: ExchangeData + Ord + ::std::fmt::Debug>(
    correct: &Stream<S, T>,
    output: &Stream<S, T>,
) -> Stream<S, ()> {
    use std::collections::HashMap;
    use timely::dataflow::channels::pact::Exchange;
    let mut in1_pending: HashMap<_, Vec<_>> = Default::default();
    let mut in2_pending: HashMap<_, Vec<_>> = Default::default();
    let mut data_buffer: Vec<T> = Vec::new();
    correct.binary_notify(
        &output,
        Exchange::new(|_| 0),
        Exchange::new(|_| 0),
        "Verify",
        vec![],
        move |in1, in2, _out, not| {
            in1.for_each(|time, data| {
                data.swap(&mut data_buffer);
                in1_pending
                    .entry(time.time().clone())
                    .or_insert_with(Default::default)
                    .extend(data_buffer.drain(..));
                not.notify_at(time.retain());
            });
            in2.for_each(|time, data| {
                data.swap(&mut data_buffer);
                in2_pending
                    .entry(time.time().clone())
                    .or_insert_with(Default::default)
                    .extend(data_buffer.drain(..));
                not.notify_at(time.retain());
            });
            not.for_each(|time, _, _| {
                let mut v1 = in1_pending.remove(time.time()).unwrap_or_default();
                let mut v2 = in2_pending.remove(time.time()).unwrap_or_default();
                v1.sort();
                v2.sort();
                assert_eq!(v1.len(), v2.len());
                let i1 = v1.iter();
                let i2 = v2.iter();
                for (a, b) in i1.zip(i2) {
                    //                    println!("a: {:?}, b: {:?}", a, b);
                    assert_eq!(a, b, " at {:?}", time.time());
                }
            })
        },
    )
}

fn main() {
    let matches = App::new("word_count")
        .arg(
            Arg::with_name("rate")
                .long("rate")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("duration")
                .long("duration")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("queries")
                .long("queries")
                .takes_value(true)
                .required(true)
                .multiple(true)
                .value_delimiter(" "),
        )
        .arg(Arg::with_name("timely").multiple(true))
        .get_matches();
    let timely_args = matches
        .values_of("timely")
        .map_or(Vec::new(), |vs| vs.map(String::from).collect());

    let rate: u64 = matches
        .value_of("rate")
        .expect("rate absent")
        .parse::<u64>()
        .expect("couldn't parse rate");

    let duration_ns: u64 = matches
        .value_of("duration")
        .expect("duration absent")
        .parse::<u64>()
        .expect("couldn't parse duration")
        * 1_000_000_000;

    let queries: Vec<_> = matches
        .values_of("queries")
        .unwrap()
        .map(String::from)
        .collect();

    // Read and report RSS
    let statm_reporter_running = nexmark::tools::statm_reporter();

    // define a new computational scope, in which to run NEXMark queries
    let timelines: Vec<_> = timely::execute_from_args(timely_args.into_iter(), move |worker| {
        let peers = worker.peers();
        let index = worker.index();

        // Declare re-used input, control and probe handles.
        let mut input = InputHandle::new();
        //let mut control_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        {
            let bids = std::rc::Rc::new(
                timely::dataflow::operators::capture::event::link::EventLink::new(),
            );
            let auctions = std::rc::Rc::new(
                timely::dataflow::operators::capture::event::link::EventLink::new(),
            );
            let people = std::rc::Rc::new(
                timely::dataflow::operators::capture::event::link::EventLink::new(),
            );

            let closed_auctions = std::rc::Rc::new(
                timely::dataflow::operators::capture::event::link::EventLink::new(),
            );
            let closed_auctions_flex = std::rc::Rc::new(
                timely::dataflow::operators::capture::event::link::EventLink::new(),
            );

            let nexmark_input = NexmarkInput {
                //control: &control,
                bids: &bids,
                auctions: &auctions,
                people: &people,
                closed_auctions: &closed_auctions,
                closed_auctions_flex: &closed_auctions_flex,
            };

            let nexmark_timer = NexmarkTimer {
                time_dilation: TIME_DILATION,
            };

            worker.dataflow(|scope: &mut ::timely::dataflow::scopes::Child<_, usize>| {
                use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
                let mut demux = OperatorBuilder::new("NEXMark demux".to_string(), scope.clone());

                let mut input = demux.new_input(&input.to_stream(scope), Pipeline);

                let (mut b_out, bids_stream) = demux.new_output();
                let (mut a_out, auctions_stream) = demux.new_output();
                let (mut p_out, people_stream) = demux.new_output();

                let mut demux_buffer = Vec::new();

                demux.build(move |_capability| {
                    move |_frontiers| {
                        let mut b_out = b_out.activate();
                        let mut a_out = a_out.activate();
                        let mut p_out = p_out.activate();

                        input.for_each(|time, data| {
                            data.swap(&mut demux_buffer);
                            let mut b_session = b_out.session(&time);
                            let mut a_session = a_out.session(&time);
                            let mut p_session = p_out.session(&time);

                            for datum in demux_buffer.drain(..) {
                                match datum {
                                    nexmark::event::Event::Bid(b) => b_session.give(b),
                                    nexmark::event::Event::Auction(a) => a_session.give(a),
                                    nexmark::event::Event::Person(p) => p_session.give(p),
                                }
                            }
                        });
                    }
                });

                bids_stream.capture_into(bids.clone());
                auctions_stream.capture_into(auctions.clone());
                people_stream.capture_into(people.clone());
            });

            // Q3: Join some auctions. Native.
            if queries.iter().any(|x| *x == "q3") {
                worker.dataflow(|scope| {
                    ::nexmark::queries::q3(&nexmark_input, nexmark_timer, scope)
                        .probe_with(&mut probe);
                });
            }

            // Q4: Find average selling price per category. Native.
            if queries.iter().any(|x| *x == "q4") {
                worker.dataflow(|scope| {
                    ::nexmark::queries::q4_q6_common(&nexmark_input, nexmark_timer, scope)
                        .capture_into(nexmark_input.closed_auctions.clone());
                    ::nexmark::queries::q4(&nexmark_input, nexmark_timer, scope)
                        .probe_with(&mut probe);
                });
            }

            // Q4: Find average selling price per category. Flex.
            if queries.iter().any(|x| *x == "q4_flex") {
                worker.dataflow(|scope| {
                    ::nexmark::queries::q4_q6_common(&nexmark_input, nexmark_timer, scope)
                        .capture_into(nexmark_input.closed_auctions.clone());
                    ::nexmark::queries::q4_flex(&nexmark_input, nexmark_timer, scope)
                        .probe_with(&mut probe);
                });
            }

            // Q5. Hot Items. Native.
            if queries.iter().any(|x| *x == "q5") {
                // 60s windows, ticking in 1s intervals
                // NEXMark default is 60 minutes, ticking in one minute intervals
                let window_slice_count = 60;
                let window_slide_ns = 1_000_000_000;
                worker.dataflow(|scope| {
                    ::nexmark::queries::q5(
                        &nexmark_input,
                        nexmark_timer,
                        scope,
                        window_slice_count,
                        window_slide_ns,
                    )
                    .probe_with(&mut probe);
                });
            }

            // Q5. Hot Items. Index.
            if queries.iter().any(|x| *x == "q5_index") {
                // 60s windows, ticking in 1s intervals
                // NEXMark default is 60 minutes, ticking in one minute intervals
                let window_slice_count = 60;
                let window_slide_ns = 1_000_000_000;
                worker.dataflow(|scope| {
                    ::nexmark::queries::q5_index(
                        &nexmark_input,
                        nexmark_timer,
                        scope,
                        window_slice_count,
                        window_slide_ns,
                    )
                        .probe_with(&mut probe);
                });
            }

            // Q6. Avg selling price per seller. Native.
            if queries.iter().any(|x| *x == "q6") {
                worker.dataflow(|scope| {
                    ::nexmark::queries::q4_q6_common(&nexmark_input, nexmark_timer, scope)
                        .capture_into(nexmark_input.closed_auctions.clone());
                    ::nexmark::queries::q6(&nexmark_input, nexmark_timer, scope)
                        .probe_with(&mut probe);
                });
            }

            // Q7. Highest Bid. Native.
            if queries.iter().any(|x| *x == "q7") {
                worker.dataflow(|scope| {
                    // Window ticks every 10 seconds.
                    // NEXMark default is different: ticks every 60s
                    let window_size_ns = 10_000_000_000;
                    ::nexmark::queries::q7(&nexmark_input, nexmark_timer, scope, window_size_ns)
                        .probe_with(&mut probe);
                });
            }

            // Q8. Monitor new users. Native.
            if queries.iter().any(|x| *x == "q8") {
                worker.dataflow(|scope| {
                    // Window ticks every 12 minutes.
                    // NEXMark default is different: ticks every 12h
                    let window_size_ns = 720 * 1_000_000_000;
                    ::nexmark::queries::q8(&nexmark_input, nexmark_timer, scope, window_size_ns)
                        .probe_with(&mut probe);
                });
            }
        }

        let mut config1 = nexmark::config::Config::new();
        // 0.06*60*60*12 = 0.06*60*60*12
        // auction_proportion*sec_in_12h
        config1.insert("in-flight-auctions", format!("{}", rate * 2592));
        config1.insert("events-per-second", format!("{}", rate));
        config1.insert("first-event-number", format!("{}", index));
        let mut config = nexmark::config::NEXMarkConfig::new(&config1);

        let count = 1;
        input.advance_to(count);
        while probe.less_than(&count) {
            worker.step();
        }

        let timer = ::std::time::Instant::now();

        // Establish a start of the computation.
        let elapsed_ns = timer.elapsed().to_nanos();
        config.base_time_ns = elapsed_ns as usize;

        use rand::rngs::SmallRng;
        use rand::SeedableRng;
        assert!(worker.peers() < 256);
        let mut rng = SmallRng::from_seed([worker.peers() as u8; 16]);

        let input_times = {
            let config = config.clone();
            move || {
                nexmark::config::NexMarkInputTimes::new(
                    config.clone(),
                    duration_ns,
                    TIME_DILATION,
                    peers,
                )
            }
        };

        let mut output_metric_collector =
            ::streaming_harness::output::default::hdrhist_timeline_collector(
                input_times(),
                0,
                2_000_000_000,
                duration_ns - 2_000_000_000,
                duration_ns,
                250_000_000,
            );

        let mut events_so_far = 0;

        let mut input_times_gen =
            ::streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

        let mut input = Some(input);

        let mut last_ns = 0;

        loop {
            let elapsed_ns = timer.elapsed().to_nanos();
            let wait_ns = last_ns;
            let target_ns = (elapsed_ns + 1) / 1_000_000 * 1_000_000;
            last_ns = target_ns;

            output_metric_collector
                .acknowledge_while(elapsed_ns, |t| !probe.less_than(&(t as usize + count)));

            if input.is_none() {
                break;
            }

            if let Some(it) = input_times_gen.iter_until(target_ns) {
                let input = input.as_mut().unwrap();
                for _t in it {
                    input.send(Event::create(events_so_far, &mut rng, &mut config));
                    events_so_far += worker.peers();
                }
                input.advance_to(target_ns as usize + count);
            } else {
                input.take().unwrap();
            }

            if input.is_some() {
                while probe.less_than(&(wait_ns as usize + count)) {
                    worker.step();
                }
            } else {
                while worker.step() {}
            }
        }

        output_metric_collector.into_inner()
    })
    .expect("unsuccessful execution")
    .join()
    .into_iter()
    .map(|x| x.unwrap())
    .collect();

    statm_reporter_running.store(false, ::std::sync::atomic::Ordering::SeqCst);

    let ::streaming_harness::timeline::Timeline {
        timeline,
        latency_metrics,
        ..
    } = ::streaming_harness::output::combine_all(timelines);

    let latency_metrics = latency_metrics.into_inner();

    for (value, prob, count) in latency_metrics.ccdf() {
        println!("latency_ccdf\t{}\t{}\t{}", value, prob, count);
    }
    println!(
        "{}",
        ::streaming_harness::format::format_summary_timeline(
            "summary_timeline".to_string(),
            timeline.clone()
        )
    );
}
