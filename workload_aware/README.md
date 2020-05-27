## Running a Query
Each query can be run for a specified duration (in seconds) and with a given event generation rate

Queries have three varieties. To run using "vanilla" Timely simply supply the query number e.g. `q3`. To run using Managed State supply the desired state backend as a suffix e.g. `q3_faster` or `q3_mem`.
```bash
$ cargo run --release -- --duration 1000 --rate 1000000 --queries q3_faster
```

## Running on multiple workers/processes
Timely Dataflow accepts configuration via arguments supplied at runtime. These can be passed by adding an extra `--` between the line above and Timely's arguments.

For example to run with four workers:
```bash
$ cargo run --release -- --duration 1000 --rate 1000000 --queries q3_faster -- -w 4
```