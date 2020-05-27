use streaming_harness::util::ToNanos;

pub fn statm_reporter() -> ::std::sync::Arc<::std::sync::atomic::AtomicBool> {
    // Read and report RSS every 100ms
    let statm_reporter_running = ::std::sync::Arc::new(::std::sync::atomic::AtomicBool::new(true));
    {
        let statm_reporter_running = statm_reporter_running.clone();
        ::std::thread::spawn(move || {
            use std::io::Read;
            let timer = ::std::time::Instant::now();
            let mut iteration = 0;
            while statm_reporter_running.load(::std::sync::atomic::Ordering::SeqCst) {
                let mut stat_s = String::new();
                let mut statm_f =
                    ::std::fs::File::open("/proc/self/statm").expect("can't open /proc/self/statm");
                statm_f
                    .read_to_string(&mut stat_s)
                    .expect("can't read /proc/self/statm");
                let pages: u64 = stat_s
                    .split_whitespace()
                    .nth(1)
                    .expect("wooo")
                    .parse()
                    .expect("not a number");
                let rss = pages * 4096;

                let elapsed_ns = timer.elapsed().to_nanos();
                println!("statm_RSS\t{}\t{}", elapsed_ns, rss);
                #[allow(deprecated)]
                ::std::thread::sleep_ms(500 - (elapsed_ns / 1_000_000 - iteration * 500) as u32);
                iteration += 1;
            }
        });
    }
    statm_reporter_running
}
