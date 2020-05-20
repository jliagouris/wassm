//! NEXMark benchmark implementation.
//!
//! Query parameters based on http://datalab.cs.pdx.edu/niagara/NEXMark/ unless noted otherwise.

extern crate abomonation;
extern crate clap;
extern crate env_logger;
extern crate metrics_runtime;
extern crate nexmark;
extern crate rand;
extern crate streaming_harness;
extern crate timely;

use std::alloc::System;

#[global_allocator]
static GLOBAL: System = System;

const TIME_DILATION: usize = 1;

use clap::{App, Arg};

use metrics_runtime::Receiver;
use streaming_harness::util::ToNanos;

use timely::dataflow::operators::{Capture, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::state::backends::{
    FASTERBackend, InMemoryBackend, RocksDBBackend, RocksDBMergeBackend, RocksDBMergeBackend2
};
use timely::ExchangeData;

use nexmark::event::Event;
use nexmark::queries::{NexmarkInput, NexmarkTimer};
//use timely::dataflow::operators::inspect::Inspect;

use std::time::Duration;
use std::io::Write;
use metrics_runtime::exporters::LogExporter;
use metrics_runtime::observers::YamlBuilder;
use log::Level;
use std::fs::File;

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
        move |in1, in2, _out, not, _state_handle| {
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
    let matches = App::new("window_evaluation")
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
            Arg::with_name("window-slice-count")
                .long("window-slice-count")
                .takes_value(true),
                //.required(true),
        )
        .arg(
            Arg::with_name("window-slide")
                .long("window-slide")
                .takes_value(true),
                //.required(true),
        )
        .arg(
            Arg::with_name("queries")
                .long("queries")
                .takes_value(true)
                .required(true)
                .multiple(true)
                .value_delimiter(" "),
        )
        .arg(
            Arg::with_name("metrics")
                .long("metrics")
        )
        .arg(
            Arg::with_name("print-rss")
                .long("print-rss")
        )
        .arg(
            Arg::with_name("latency-output")
                .long("latency-output")
                .takes_value(true)
                .required(false)
        )
        .arg(
            Arg::with_name("timeline-output")
                .long("timeline-output")
                .takes_value(true)
                .required(false)
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

    let window_slice_count: usize = matches
        .value_of("window-slice-count")
        .unwrap_or("0")
        .parse::<usize>()
        .expect("couldn't parse window slice count");

    let window_slide_ns: usize = matches
        .value_of("window-slide")
        .unwrap_or("0")
        .parse::<usize>()
        .expect("couldn't parse window slide")
        * 1_000_000_000;

    let queries: Vec<_> = matches
        .values_of("queries")
        .unwrap()
        .map(String::from)
        .collect();

    let enable_metrics = matches
        .occurrences_of("metrics") > 0;

    let enable_rss = matches
        .occurrences_of("print-rss") > 0;

    let latency_output = matches
        .value_of("latency-output");

    let timeline_output = matches
        .value_of("timeline-output");

    if enable_metrics {
        // Collect metrics
        env_logger::init();
        let receiver = Receiver::builder()
            .build()
            .expect("failed to build receiver");
        let mut exporter = LogExporter::new(receiver.controller(), YamlBuilder::new(), Level::Info, Duration::from_secs(30));
        receiver.install();
        std::thread::spawn(move ||exporter.run());
    }

    let statm_reporter_running = match enable_rss {
        // Read and report RSS
        true => Some(nexmark::tools::statm_reporter()),
        _ => None
    };

    // define a new computational scope, in which to run NEXMark queries
    let timelines: Vec<_> = timely::execute_from_args(
        timely_args.into_iter(),
        move |worker, _node_state_handle| {
            let peers = worker.peers();
            let index = worker.index();

            // Declare re-used input, control and probe handles.
            let mut input = InputHandle::new();
            //let mut control_input = InputHandle::new();
            let mut probe = ProbeHandle::new();

            {
                //let control = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());

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

                worker.dataflow(
                    |scope: &mut ::timely::dataflow::scopes::Child<_, usize, InMemoryBackend>,
                     _| {
                        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
                        let mut demux =
                            OperatorBuilder::new("NEXMark demux".to_string(), scope.clone());

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
                    },
                );

                // Q3: Join some auctions. FASTER.
                if queries.iter().any(|x| *x == "q3_faster") {
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q3_managed(&nexmark_input, nexmark_timer, scope)
                            .probe_with(&mut probe);
                    });
                }

                // Q3: Join some auctions. RocksDB.
                if queries.iter().any(|x| *x == "q3_rocksdb") {
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q3_managed(&nexmark_input, nexmark_timer, scope)
                            .probe_with(&mut probe);
                    });
                }

                // Q4: Find average selling price per category
                // Uses FASTER for the common part and an in-memory hash map for the final aggregation
                if queries.iter().any(|x| *x == "q4_flex") {
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q4_q6_common_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                        )
                            .capture_into(nexmark_input.closed_auctions.clone());
                        ::nexmark::queries::nexmark::q4(&nexmark_input, nexmark_timer, scope)
                            .probe_with(&mut probe);
                    });
                }

                // Q4: Find average selling price per category. FASTER.
                if queries.iter().any(|x| *x == "q4_faster") {
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q4_q6_common_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                        )
                            .capture_into(nexmark_input.closed_auctions.clone());
                        ::nexmark::queries::nexmark::q4_managed(&nexmark_input, nexmark_timer, scope)
                            .probe_with(&mut probe);
                    });
                }

                // Q4: Find average selling price per category. RocksDB.
                if queries.iter().any(|x| *x == "q4_rocksdb") {
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q4_q6_common_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                        )
                            .capture_into(nexmark_input.closed_auctions.clone());
                        ::nexmark::queries::nexmark::q4_managed(&nexmark_input, nexmark_timer, scope)
                            //.inspect_batch(|t,xs| println!("@{}: {:?}", t, xs))
                            .probe_with(&mut probe);
                    });
                }

                // Q5. Hot Items. FASTER.
                if queries.iter().any(|x| *x == "q5_faster") {
                    // 60s windows, ticking in 1s intervals
                    // NEXMark default is 60 minutes, ticking in one minute intervals
                    let w_slice_count = 60;
                    let w_slide_ns = 1_000_000_000;
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q5_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            w_slice_count,
                            w_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // Q5. Hot Items. FASTER.
                if queries.iter().any(|x| *x == "q5_faster_index") {
                    // 60s windows, ticking in 1s intervals
                    // NEXMark default is 60 minutes, ticking in one minute intervals
                    let w_slice_count = 60;
                    let w_slide_ns = 1_000_000_000;
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q5_managed_index(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            w_slice_count,
                            w_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // Q5. Hot Items. RocksDB.
                if queries.iter().any(|x| *x == "q5_rocksdb") {
                    // 60s windows, ticking in 1s intervals
                    // NEXMark default is 60 minutes, ticking in one minute intervals
                    let w_slice_count = 60;
                    let w_slide_ns = 1_000_000_000;
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q5_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            w_slice_count,
                            w_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // Q5. Hot Items. RocksDB.
                if queries.iter().any(|x| *x == "q5_rocksdb_index") {
                    // 60s windows, ticking in 1s intervals
                    // NEXMark default is 60 minutes, ticking in one minute intervals
                    let w_slice_count = 60;
                    let w_slide_ns = 1_000_000_000;
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q5_managed_index(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            w_slice_count,
                            w_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // Q6. Avg selling price per seller. FASTER.
                if queries.iter().any(|x| *x == "q6_faster") {
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q4_q6_common_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                        )
                            .capture_into(nexmark_input.closed_auctions.clone());
                        ::nexmark::queries::nexmark::q6_managed(&nexmark_input, nexmark_timer, scope)
                            .probe_with(&mut probe);
                    });
                }

                // Q6. Avg selling price per seller. RocksDB.
                if queries.iter().any(|x| *x == "q6_rocksdb") {
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::nexmark::q4_q6_common_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                        )
                            .capture_into(nexmark_input.closed_auctions.clone());
                        ::nexmark::queries::nexmark::q6_managed(&nexmark_input, nexmark_timer, scope)
                            .probe_with(&mut probe);
                    });
                }

                // Q7. Highest Bid. FASTER.
                if queries.iter().any(|x| *x == "q7_faster") {
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        // Window ticks every 10 seconds.
                        // NEXMark default is different: ticks every 60s
                        let window_size_ns = 10_000_000_000;
                        ::nexmark::queries::nexmark::q7_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_size_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // Q7. Highest Bid. RocksDB.
                if queries.iter().any(|x| *x == "q7_rocksdb") {
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        // Window ticks every 10 seconds.
                        // NEXMark default is different: ticks every 60s
                        let window_size_ns = 10_000_000_000;
                        ::nexmark::queries::nexmark::q7_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_size_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // Q8. Monitor new users. FASTER.
                if queries.iter().any(|x| *x == "q8_faster") {
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                        ::nexmark::queries::nexmark::q8_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_size_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // Q8. Monitor new users. RocksDB.
                if queries.iter().any(|x| *x == "q8_rocksdb") {
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                        //let window_size_ns = 4 * 1_000_000_000;
                        ::nexmark::queries::nexmark::q8_managed(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_size_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // Q8. Monitor new users. FASTER.
                if queries.iter().any(|x| *x == "q8_faster_map") {
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                        ::nexmark::queries::nexmark::q8_managed_map(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_size_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // Q8. Monitor new users. RocksDB.
                if queries.iter().any(|x| *x == "q8_rocksdb_map") {
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                        //let window_size_ns = 4 * 1_000_000_000;
                        ::nexmark::queries::nexmark::q8_managed_map(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_size_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with FASTER
                if queries.iter().any(|x| *x == "window_1_faster") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_1_faster(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with FASTER and COUNT aggregation
                if queries.iter().any(|x| *x == "window_1_faster_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_1_faster_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with FASTER and COUNT aggregation using custom slicing
                if queries.iter().any(|x| *x == "window_1_faster_count_custom_slice") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_1_faster_count_custom_slice(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with FASTER and count aggregation using custom slicing
                if queries.iter().any(|x| *x == "keyed_window_1_faster_count_custom_slice") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_1_faster_count_custom_slice(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with FASTER and RANK aggregation
                if queries.iter().any(|x| *x == "window_1_faster_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_1_faster_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with FASTER and RANK aggregation using custom slicing
                if queries.iter().any(|x| *x == "window_1_faster_rank_custom_slice") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_1_faster_rank_custom_slice(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with FASTER and count aggregation using custom slicing
                if queries.iter().any(|x| *x == "keyed_window_1_faster_rank_custom_slice") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_1_faster_rank_custom_slice(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with FASTER
                if queries.iter().any(|x| *x == "window_2_faster") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2_faster(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with FASTER and COUNT aggregation
                if queries.iter().any(|x| *x == "window_2_faster_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2_faster_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with FASTER and RANK aggregation
                if queries.iter().any(|x| *x == "window_2_faster_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2_faster_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with FASTER and RANK aggregation
                if queries.iter().any(|x| *x == "keyed_window_2_faster_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_2_faster_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with FASTER
                if queries.iter().any(|x| *x == "window_3_faster") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3_faster(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with FASTER and COUNT aggregation
                if queries.iter().any(|x| *x == "window_3_faster_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3_faster_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with FASTER and COUNT aggregation
                if queries.iter().any(|x| *x == "keyed_window_3_faster_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_3_faster_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with FASTER and RANK aggregation
                if queries.iter().any(|x| *x == "window_3_faster_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, FASTERBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3_faster_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with RocksDB
                if queries.iter().any(|x| *x == "window_1_rocksdb") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_1_rocksdb(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with RocksDB and COUNT aggregation
                if queries.iter().any(|x| *x == "window_1_rocksdb_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_1_rocksdb_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with RocksDB and COUNT aggregation
                if queries.iter().any(|x| *x == "keyed_window_1_rocksdb_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_1_rocksdb_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with RocksDB and RANK aggregation
                if queries.iter().any(|x| *x == "window_1_rocksdb_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_1_rocksdb_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 1st window implementation with RocksDB and RANK aggregation
                if queries.iter().any(|x| *x == "keyed_window_1_rocksdb_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_1_rocksdb_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with RocksDB using put + get
                if queries.iter().any(|x| *x == "window_2a_rocksdb") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2a_rocksdb(  // Same implementation with FASTER
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with RocksDB using put + get and COUNT
                if queries.iter().any(|x| *x == "window_2a_rocksdb_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2a_rocksdb_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with RocksDB using merge and RANK
                if queries.iter().any(|x| *x == "keyed_window_2a_rocksdb_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_2a_rocksdb_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with RocksDB using put + get and RANK
                if queries.iter().any(|x| *x == "window_2a_rocksdb_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2a_rocksdb_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with RocksDB using merge
                if queries.iter().any(|x| *x == "window_2b_rocksdb") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBMergeBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2b_rocksdb(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with RocksDB using merge and COUNT
                if queries.iter().any(|x| *x == "window_2b_rocksdb_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBMergeBackend2>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2b_rocksdb_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with RocksDB using merge and RANK
                if queries.iter().any(|x| *x == "window_2b_rocksdb_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBMergeBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_2b_rocksdb_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 2nd window implementation with RocksDB using merge and RANK
                if queries.iter().any(|x| *x == "keyed_window_2b_rocksdb_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBMergeBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_2b_rocksdb_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with RocksDB using put + get
                if queries.iter().any(|x| *x == "window_3a_rocksdb") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3a_rocksdb(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with RocksDB using put + get
                if queries.iter().any(|x| *x == "keyed_window_3a_rocksdb_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::keyed::keyed_window_3a_rocksdb_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with RocksDB using put + get and COUNT
                if queries.iter().any(|x| *x == "window_3a_rocksdb_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3a_rocksdb_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with RocksDB using put + get and COUNT
                if queries.iter().any(|x| *x == "window_3a_rocksdb_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3a_rocksdb_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with RocksDB using merge
                if queries.iter().any(|x| *x == "window_3b_rocksdb") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBMergeBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3b_rocksdb(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                        .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with RocksDB using merge and COUNT
                if queries.iter().any(|x| *x == "window_3b_rocksdb_count") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBMergeBackend2>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3b_rocksdb_count(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
                            .probe_with(&mut probe);
                    });
                }

                // 3rd window implementation with RocksDB using merge and COUNT
                if queries.iter().any(|x| *x == "window_3b_rocksdb_rank") {
                    assert!(window_slice_count > 0);
                    assert!(window_slide_ns > 0);
                    worker.dataflow::<_, _, _, RocksDBMergeBackend>(|scope, _| {
                        ::nexmark::queries::windows::global::window_3b_rocksdb_rank(
                            &nexmark_input,
                            nexmark_timer,
                            scope,
                            window_slice_count,
                            window_slide_ns,
                        )
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
                        let event = Event::create(events_so_far, &mut rng, &mut config);
                        //println!("Event timestamp: {:?}, epoch: {}", event.time(), target_ns as usize + count);
                        input.send(event);
                        events_so_far += worker.peers();
                    }
                    //println!("Epoch: {}", target_ns as usize + count);
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
        },
    )
    .expect("unsuccessful execution")
    .join()
    .into_iter()
    .map(|x| x.unwrap())
    .collect();

    match statm_reporter_running {
        Some(statm_reporter_running) => statm_reporter_running.store(false, ::std::sync::atomic::Ordering::SeqCst),
        _ => {}
    }

    let ::streaming_harness::timeline::Timeline {
        timeline,
        latency_metrics,
        ..
    } = ::streaming_harness::output::combine_all(timelines);

    let latency_metrics = latency_metrics.into_inner();
    /*
    println!(
        "DEBUG_summary\t{}",
        latency_metrics
            .summary_string()
            .replace("\n", "\nDEBUG_summary\t")
    );
    println!(
        "{}",
        timeline
            .clone()
            .into_iter()
            .map(
                |::streaming_harness::timeline::TimelineElement {
                     time,
                     metrics,
                     samples,
                 }| format!(
                    "DEBUG_timeline\t-- {} ({} samples) --\nDEBUG_timeline\t{}",
                    time,
                    samples,
                    metrics.summary_string().replace("\n", "\nDEBUG_timeline\t")
                )
            )
            .collect::<Vec<_>>()
            .join("\n")
    );
    */

    if let Some(output_file) = latency_output {
        let mut f = File::create(output_file).expect("Cannot open latency output file");
        for (value, prob, count) in latency_metrics.ccdf() {
            f.write(format!("latency_ccdf\t{}\t{}\t{}\n",value, prob, count).as_bytes()).ok();
        }
    } else {
        for (value, prob, count) in latency_metrics.ccdf() {
            println!("latency_ccdf\t{}\t{}\t{}", value, prob, count);
        }
    }

    if let Some(output_file) = timeline_output {
        let mut f = File::create(output_file).expect("Cannot open timeline output file");
        f.write(::streaming_harness::format::format_summary_timeline(
                "summary_timeline".to_string(),
                timeline.clone()
            ).as_bytes()).ok();
    } else {
        println!(
            "{}",
            ::streaming_harness::format::format_summary_timeline(
                "summary_timeline".to_string(),
                timeline.clone()
            )
        );
    }
}
