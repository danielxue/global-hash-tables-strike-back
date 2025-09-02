# Global Hash Table Strike Back
The repository contains the code and data for our paper (pending review): 

Daniel Xue and Ryan Marcus. 2025. [Global Hash Tables Strike Back! An Analysis of Parallel GROUP BY Aggregation](https://arxiv.org/abs/2505.04153). arXiv:2505.04153 [cs]

## Contents
This repository contains various interchangeable implementations for our two described stages of fully concurrent group aggregation as well as for partitioned group aggregation. The project is organized as followed:
- `common/`: code used throughout the project providing some sort of primitive logic.
- `experiments/`: code, scripts, data, and notebook to reproduce our experiment results.
- `partial-aggregate-update/`: a module implementing the second stage of our fully concurrent aggregation method, partial aggregate update.
- `partitioned-aggregate/`: a module implementing partitioned group aggregations.
- `ticket/`: a module implementing the first stage of our fully concurrent aggregation method, ticketing. 

## Usage
There is only one binary in the package experiments. You can run it with cargo and list additional subcommands after. We run all experiments in release mode with the compiler flag `target-cpu=native`. This would translate to `RUSTFLAGS="-C target-cpu=native" cargo run --release` to run any workloads.

### Bench
This command benchmarks the latency of given workload(s). 
```
bench
    -w, --workload <WORKLOAD>          [possible values: listed below]
    -t, --threads <THREADS>
    -e, --elements <ELEMENTS>
    -k, --keys <KEYS>
        --capacity <CAPACITY>          [optional, overwrites default capacity (the number of keys)]
        --value-type <VALUE_TYPE>      [default: i64] [possible values: i16, i32, i64]
        --operator <OPERATOR>          [default: sum] [possible values: count, max, sum, avg]
        --zipf <ZIPF>                  [optional, zipfian exponential parameter]
        --heavy-hitter <HEAVY_HITTER>  [optional, proportion of values to make the heavy hitter value]
        --no-zero                      [disables default optimization to zero-allocate large data structures]
    -i, --iterations <ITERATIONS>      [default: 5]
        --table                        [return as comma separated values]
```

For example, to benchmark the end-to-end performance of thread local aggregation with 1000 keys and 100 million elements with 32 threads, you could run the command `RUSTFLAGS="-C target-cpu=native" cargo run --release -- bench -w thread-local-e2e -t 32 -k 1000 -e 100000000`. Note that you can run multiple workloads in one go with repeated workload arguments. 

### Profile
This command behaves similarly to bench but is tuned for performance counter measurements or profiling (with `perf`). A control file handle is accepted to prevent pollution of performance counters with setup and warmup routines. 
```
profile
    -w, --workload <WORKLOAD>          [possible values: listed below]
    -t, --threads <THREADS>
    -e, --elements <ELEMENTS>
    -k, --keys <KEYS>
        --capacity <CAPACITY>          [optional, overwrites default capacity (the number of keys)]
        --value-type <VALUE_TYPE>      [default: i64] [possible values: i16, i32, i64]
        --operator <OPERATOR>          [default: sum] [possible values: count, max, sum, avg]
        --zipf <ZIPF>                  [optional, zipfian exponential parameter]
        --heavy-hitter <HEAVY_HITTER>  [optional, proportion of values to make the heavy hitter value]
        --no-zero                      [disables default optimization to zero-allocate large data structures]
    -i, --iterations <ITERATIONS>      [default: 5]
    -d, --delay <DELAY>                [default: 0]
    -c, --control <CONTROL>            [default: /dev/null]
```

For example, you could use this command with `perf` to measure performance counters as such, `RUSTFLAGS="-C target-cpu=native" perf stat --control fd:$ctl_fd -- cargo run --release -- profile -c /tmp/perf_ctl.fifo -w thread-local-e2e -t 32 -k 1000 -e 100000000`, given a FIFO pipe at `/tmp/perf_ctl.fifo` with file descriptor `$ctl_fd`.

### Workloads
The accepted workloads are as followed (note not all are shown in the paper):
- Ticketing: `cuckoo-map`, `dash-map`, `folklore-map`, `folklore-unfuzzy-map` (for fuzzy ticketing experiment only), `iceberg-map`, `leap-map`, `global-locking-map`, `once-lock-map`.
- Partial aggregate update: `atomic-pau`, `locking-pau`, `global-locking-pau`, `thread-local-pau`.
- End-to-end fully concurrent aggregation:  `atomic-e2e`, `locking-e2e`, `global-locking-e2e`, `thread-local-e2e`.
- End-to-end partitioned aggregation: `partitioned-e2e`.

## Experiments
### Data
Data from our experiments used to generate the graphs in our paper are provided in `experiments/data/`. We performed benchmarks on the following systems:
- AMD (primary system, data files not post-fixed): AMD EPYC 9454P processor with 48 cores @2.75GHz and 256 GB of RAM.
- ARM (data files post-fixed with `arm`): Ampere Altra Q80-30 processor with 80 cores @3.0GHz and 256 GB of RAM.
- Intel (data files post-fixed with `intel`): Intel Xeon Gold 5412U processor with 24 cores @2.10GHz and 256 GB of RAM.

### Reproduction
You can directly run the provided scripts in `experiments/scripts/` to reproduce the experimental results. All scripts take a list of thread counts as input. The experiments are as follows:
- `scaling_experiment.sh`: the main experiment that measures the performance and behavior of our aggregation methods.
- `stat_experiment.sh`: measures performance counters using `perf stat` on the same workloads as the scaling experiment.
- `fuzzy_ticketing_experiment.sh`: to test the impact of the fuzzy ticketer on performance.
- `resizing_experiment.sh`: to test the impact of imprecise cardinality estimation on performance.
- `tuple_size_experiment.sh`: to test the impact of tuple size on performance.
- `zeroed_experiment.sh`: to test the impact zero-allocation on performance.
