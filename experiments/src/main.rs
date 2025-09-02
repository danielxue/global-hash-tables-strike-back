use std::ops::AddAssign;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI16, AtomicI32, AtomicI64};

use atomic_traits::{Atomic, NumOps};
use bytemuck::Zeroable;
use clap::{Parser, Subcommand, ValueEnum};
use clap_derive::Args;
use itertools::Itertools;
use medians::{Median, Medianf64};
use num_traits::{FromPrimitive, NumCast};

use experiments::*;
use ticket::*;
use partitioned_aggregate::*;
use partial_aggregate_update::*;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Bench {
        #[arg(short, long, value_enum)]
        workload: Vec<Workload>,

        #[command(flatten)]
        workload_args: WorkloadArgs,

        #[arg(long)]
        table: bool,
    },
    Profile {
        #[arg(short, long, value_enum)]
        workload: Workload,

        #[command(flatten)]
        workload_args: WorkloadArgs,

        #[arg(short, long, default_value_t = 0)]
        delay: usize,

        #[arg(short, long, default_value = "/dev/null")]
        control: PathBuf,
    },
}

#[derive(Args, Debug)]
struct WorkloadArgs {
    #[arg(short, long)]
    threads: usize,

    #[arg(short, long)]
    elements: usize,

    #[arg(short, long)]
    keys: usize,

    #[arg(long)]
    capacity: Option<usize>,

    #[arg(long)]
    #[clap(value_enum, default_value_t=ValueType::I64)]
    value_type: ValueType,

    #[arg(long)]
    #[clap(value_enum, default_value_t=Operator::Sum)]
    operator: Operator,

    #[arg(long, group = "distribution")]
    zipf: Option<f64>,

    #[arg(long, group = "distribution")]
    heavy_hitter: Option<f64>,

    #[arg(long)]
    no_zero: bool,

    #[arg(short, long, default_value_t = 5)]
    iterations: usize,
}

#[derive(ValueEnum, Debug, Copy, Clone)]
#[clap(rename_all = "kebab_case")]
enum Workload {
    CuckooMap,
    DashMap,
    FolkloreMap,
    FolkloreUnfuzzyMap,
    IcebergMap,
    LeapMap,
    GlobalLockingMap,
    OnceLockMap,

    AtomicPau,
    LockingPau,
    GlobalLockingPau,
    ThreadLocalPau,

    AtomicE2E,
    LockingE2E,
    GlobalLockingE2E,
    ThreadLocalE2E,

    PartitionedE2E,
}

#[derive(ValueEnum, Debug, Copy, Clone)]
enum ValueType {
    I16,
    I32,
    I64,
}

#[derive(ValueEnum, Debug, Copy, Clone)]
enum Operator {
    Count,
    Max,
    Sum,
    Avg,
}

enum Breakdown {
    Ticketing((f64, f64, f64)),
    Update((f64, f64, f64)),
    E2EAggregation(Option<(f64, Vec<Vec<f64>>, Vec<Vec<f64>>, f64, f64)>),
    E2EPartitionedAggregation((f64, f64, f64, f64)),
}

type DefaultTicketer = KeyedFolkloreTicketer<AtomicI64>;

fn box_ticketing_workload<T: Ticketer<i64>, V>(
    threads: usize,
    keys: usize,
    capacity: Option<usize>,
    zeroed: bool,
) -> Box<dyn Fn(&mut (Vec<i64>, Vec<V>)) -> Breakdown> {
    Box::new(move |kv| {
        Breakdown::Ticketing(ticketing_workload::<T, V>(threads, keys, &kv.0, &kv.1, capacity, zeroed))
    })
}

fn box_update_workload<U: Updater<V>, V: Send + Sync>(
    threads: usize,
    keys: usize,
    capacity: Option<usize>,
    zeroed: bool,
) -> Box<dyn Fn(&mut (Vec<i64>, Vec<V>)) -> Breakdown> {
    Box::new(move |kv| {
        Breakdown::Update(update_workload::<U, V>(threads, keys, &kv.0, &kv.1, capacity, zeroed))
    })
}

fn box_e2e_workload<T: KeyedTicketer<i64>, U: Updater<V>, V: Send + Sync>(
    threads: usize,
    keys: usize,
    capacity: Option<usize>,
    zeroed: bool,
) -> Box<dyn Fn(&mut (Vec<i64>, Vec<V>)) -> Breakdown>{
    Box::new(move |kv| {
        Breakdown::E2EAggregation(end_to_end_workload::<T, U, V>(threads, keys, &kv.0, &kv.1, capacity, zeroed))
    })
}

fn box_e2e_partitioned_workload<A: PartitionedAggregator<i64, V>, V: Send + Sync>(
    threads: usize,
    keys: usize,
    capacity: Option<usize>,
    zeroed: bool,
) -> Box<dyn Fn(&mut (Vec<i64>, Vec<V>)) -> Breakdown> {
    Box::new(move |kv| {
        Breakdown::E2EPartitionedAggregation(partitioned_end_to_end_workload::<A, V>(threads, keys, &kv.0, &kv.1, capacity, zeroed))
    })
}

fn box_workload_closure<K: Send + Sync, V: Send + Sync + Atomic + NumOps + Zeroable>(
    workload: Workload,
    operator: Operator,
    threads: usize,
    keys: usize,
    capacity: Option<usize>,
    zeroed: bool,
) -> Box<dyn Fn(&mut (Vec<i64>, Vec<<V as Atomic>::Type>)) -> Breakdown>
where <V as Atomic>::Type: Send + Sync + PartialOrd + NumCast + AddAssign + Clone + Zeroable + Default + 'static,
{
    match (workload, operator) {
        (Workload::CuckooMap, _)
        | (Workload::DashMap, _)
        | (Workload::FolkloreMap, _)
        | (Workload::FolkloreUnfuzzyMap, _)
        | (Workload::IcebergMap, _)
        | (Workload::LeapMap, _)
        | (Workload::GlobalLockingMap, _)
        | (Workload::OnceLockMap, _) => {
            match workload {
                Workload::CuckooMap => box_ticketing_workload::<CuckooTicketer<i64>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::DashMap => box_ticketing_workload::<DashTicketer<i64>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::FolkloreMap => box_ticketing_workload::<FolkloreTicketer<AtomicI64>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::FolkloreUnfuzzyMap => box_ticketing_workload::<FolkloreUnfuzzyTicketer<AtomicI64>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::IcebergMap => box_ticketing_workload::<IcebergTicketer<i64>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::LeapMap => box_ticketing_workload::<LeapTicketer<i64>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::GlobalLockingMap => box_ticketing_workload::<GlobalLockingTicketer<i64>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::OnceLockMap => box_ticketing_workload::<OnceLockHashMap<i64>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                _ => panic!("Unimplemented workload: {:?} {:?}", workload, operator),
            }
        },
        (Workload::AtomicPau, Operator::Count)
        | (Workload::LockingPau, Operator::Count)
        | (Workload::GlobalLockingPau, Operator::Count)
        | (Workload::ThreadLocalPau, Operator::Count) => {
            match workload {
                Workload::AtomicPau => box_update_workload::<atomic_pau::CountUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::LockingPau => box_update_workload::<locking_pau::CountUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::GlobalLockingPau => box_update_workload::<global_locking_pau::CountUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::ThreadLocalPau => box_update_workload::<thread_local_pau::CountUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                _ => panic!("Unimplemented workload: {:?} {:?}", workload, operator),
            }
        },
        (Workload::AtomicPau, Operator::Max)
        | (Workload::LockingPau, Operator::Max)
        | (Workload::GlobalLockingPau, Operator::Max)
        | (Workload::ThreadLocalPau, Operator::Max) => {
            match workload {
                Workload::AtomicPau => box_update_workload::<atomic_pau::MaxUpdater<V>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::LockingPau => box_update_workload::<locking_pau::MaxUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::GlobalLockingPau => box_update_workload::<global_locking_pau::MaxUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::ThreadLocalPau => box_update_workload::<thread_local_pau::MaxUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                _ => panic!("Unimplemented workload: {:?} {:?}", workload, operator),
            }
        },
        (Workload::AtomicPau, Operator::Avg) => {
            box_update_workload::<atomic_pau::AvgUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed)
        },
        (Workload::AtomicPau, Operator::Sum)
        | (Workload::LockingPau, Operator::Sum)
        | (Workload::GlobalLockingPau, Operator::Sum)
        | (Workload::ThreadLocalPau, Operator::Sum) => {
            match workload {
                Workload::AtomicPau => box_update_workload::<atomic_pau::SumUpdater<V>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::LockingPau => box_update_workload::<locking_pau::SumUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::GlobalLockingPau => box_update_workload::<global_locking_pau::SumUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::ThreadLocalPau => box_update_workload::<thread_local_pau::SumUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                _ => panic!("Unimplemented workload: {:?} {:?}", workload, operator),
            }
        },
        (Workload::AtomicE2E, Operator::Count)
        | (Workload::LockingE2E, Operator::Count)
        | (Workload::GlobalLockingE2E, Operator::Count)
        | (Workload::ThreadLocalE2E, Operator::Count) => {
            match workload {
                Workload::AtomicE2E => box_e2e_workload::<DefaultTicketer, atomic_pau::CountUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::LockingE2E => box_e2e_workload::<DefaultTicketer, locking_pau::CountUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::GlobalLockingE2E => box_e2e_workload::<DefaultTicketer, global_locking_pau::CountUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::ThreadLocalE2E => box_e2e_workload::<DefaultTicketer, thread_local_pau::CountUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                _ => panic!("Unimplemented workload: {:?} {:?}", workload, operator),
            }
        },
        (Workload::AtomicE2E, Operator::Max)
        | (Workload::LockingE2E, Operator::Max)
        | (Workload::GlobalLockingE2E, Operator::Max)
        | (Workload::ThreadLocalE2E, Operator::Max) => {
            match workload {
                Workload::AtomicE2E => box_e2e_workload::<DefaultTicketer, atomic_pau::MaxUpdater<V>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::LockingE2E => box_e2e_workload::<DefaultTicketer, locking_pau::MaxUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::GlobalLockingE2E => box_e2e_workload::<DefaultTicketer, global_locking_pau::MaxUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::ThreadLocalE2E => box_e2e_workload::<DefaultTicketer, thread_local_pau::MaxUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                _ => panic!("Unimplemented workload: {:?} {:?}", workload, operator),
            }
        },
        (Workload::AtomicE2E, Operator::Sum)
        | (Workload::LockingE2E, Operator::Sum)
        | (Workload::GlobalLockingE2E, Operator::Sum)
        | (Workload::ThreadLocalE2E, Operator::Sum) => {
            match workload {
                Workload::AtomicE2E => box_e2e_workload::<DefaultTicketer, atomic_pau::SumUpdater<V>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::LockingE2E => box_e2e_workload::<DefaultTicketer, locking_pau::SumUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::GlobalLockingE2E => box_e2e_workload::<DefaultTicketer, global_locking_pau::SumUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Workload::ThreadLocalE2E => box_e2e_workload::<DefaultTicketer, thread_local_pau::SumUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                _ => panic!("Unimplemented workload: {:?} {:?}", workload, operator),
            }
        },
        (Workload::AtomicE2E, Operator::Avg) => {
            box_e2e_workload::<DefaultTicketer, atomic_pau::AvgUpdater<<V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed)
        },
        (Workload::PartitionedE2E, _) => {
            match operator {
                Operator::Count => box_e2e_partitioned_workload::<basic_partitioned_agg::SumAgg<i64, <V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Operator::Max => box_e2e_partitioned_workload::<basic_partitioned_agg::SumAgg<i64, <V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                Operator::Sum => box_e2e_partitioned_workload::<basic_partitioned_agg::SumAgg<i64, <V as Atomic>::Type>, <V as Atomic>::Type>(threads, keys, capacity, zeroed),
                _ => panic!("Unimplemented workload: {:?} {:?}", workload, operator),
            }
        },
        _ => panic!("Unimplemented workload: {:?}", workload),
    }
}

fn process_breakdown(breakdown: Vec<Breakdown>) -> (f64, f64, f64, f64) {
    match breakdown[0] {
        Breakdown::Ticketing(_) => {
            let res = breakdown.into_iter()
                .map(|b| {
                    if let Breakdown::Ticketing(b_unwrapped) = b {
                        b_unwrapped
                    } else {
                        panic!("Inconsistent breakdown types");
                    }
                })
                .collect_vec();

            let mut init_times = Vec::new();
            let mut ticketing_times = Vec::new();
            let mut mat_times = Vec::new();

            res.into_iter()
                .for_each(|(initialization, ticketing, materialization)| {
                    init_times.push(initialization);
                    ticketing_times.push(ticketing);
                    mat_times.push(materialization);
                });

            (
                init_times.as_mut_slice().medf_unchecked(),
                ticketing_times.as_mut_slice().medf_unchecked(),
                0f64,
                mat_times.as_mut_slice().medf_unchecked(),
            )
        },
        Breakdown::Update(_) => {
            let res = breakdown.into_iter()
                .map(|b| {
                    if let Breakdown::Update(b_unwrapped) = b {
                        b_unwrapped
                    } else {
                        panic!("Inconsistent breakdown types");
                    }
                })
                .collect_vec();

            let mut init_times = Vec::new();
            let mut pau_times = Vec::new();
            let mut mat_times = Vec::new();

            res.into_iter()
                .for_each(|(initialization, aggregation, materialization)| {
                    init_times.push(initialization);
                    pau_times.push(aggregation);
                    mat_times.push(materialization);
                });

            (
                init_times.as_mut_slice().medf_unchecked(),
                0f64,
                pau_times.as_mut_slice().medf_unchecked(),
                mat_times.as_mut_slice().medf_unchecked(),
            )
        },
        Breakdown::E2EAggregation(_) => {
            let res = breakdown.into_iter()
                .map(|b| {
                    if let Breakdown::E2EAggregation(b_unwrapped) = b {
                        b_unwrapped
                    } else {
                        panic!("Inconsistent breakdown types");
                    }
                })
                .collect_vec();

            let mut init_times = Vec::new();
            let mut ticketing_times = Vec::new();
            let mut pau_times = Vec::new();
            let mut mat_times = Vec::new();

            res.into_iter()
                .map(|trial| trial.unwrap())
                .map(|(init, s1, s2, s12, mat)| {
                    let s1 = s1.into_iter().flat_map(|thread_counts| thread_counts.into_iter()).sum::<f64>();
                    let s2 = s2.into_iter().flat_map(|thread_counts| thread_counts.into_iter()).sum::<f64>();
                    (init, (s1 / (s1 + s2)) * s12, (s2 / (s1 + s2)) * s12, mat)
                })
                .for_each(|(initialization, ticketing, aggregation, materialization)| {
                    init_times.push(initialization);
                    ticketing_times.push(ticketing);
                    pau_times.push(aggregation);
                    mat_times.push(materialization);
                });

            (
                init_times.as_mut_slice().medf_unchecked(),
                ticketing_times.as_mut_slice().medf_unchecked(),
                pau_times.as_mut_slice().medf_unchecked(),
                mat_times.as_mut_slice().medf_unchecked(),
            )
        },
        Breakdown::E2EPartitionedAggregation(_) => {
            let res = breakdown.into_iter()
                .map(|b| {
                    if let Breakdown::E2EPartitionedAggregation(b_unwrapped) = b {
                        b_unwrapped
                    } else {
                        panic!("Inconsistent breakdown types");
                    }
                })
                .collect_vec();

            let mut init_times = Vec::new();
            let mut pre_times = Vec::new();
            let mut local_times = Vec::new();
            let mut mat_times = Vec::new();

            res.into_iter()
                .for_each(|(initialization, pre, local, materialization)| {
                    init_times.push(initialization);
                    pre_times.push(pre);
                    local_times.push(local);
                    mat_times.push(materialization);
                });

            (
                init_times.as_mut_slice().medf_unchecked(),
                pre_times.as_mut_slice().medf_unchecked(),
                local_times.as_mut_slice().medf_unchecked(),
                mat_times.as_mut_slice().medf_unchecked(),
            )
        },
    }
}

fn main() {
    let args = Args::parse();

    match args.command {
        Commands::Bench { workload, workload_args, table } => {
            let WorkloadArgs { threads, elements, keys, capacity, value_type, operator, zipf, heavy_hitter, no_zero, iterations } = workload_args;

            let print_results = |results: (Vec<f64>, Vec<usize>, Vec<Breakdown>), w: Workload| {
                let results_base = results.0;
                let mut mems = results.1;
                let breakdown_results = results.2;
                let results_wl: (f64, f64, f64, f64) = process_breakdown(breakdown_results);

                if table {
                    print!(
                        "{:?},{},{},{},{},{:?},{:?},{:?},{},{},{:+e}",
                        w,
                        threads,
                        keys,
                        elements,
                        capacity.unwrap_or(keys),
                        !no_zero,
                        value_type,
                        operator,
                        zipf.unwrap_or(0.0),
                        heavy_hitter.unwrap_or(0.0),
                        mems.as_mut_slice().uqmedian(|v| u64::from_usize(*v).unwrap()).unwrap(),
                    );
                    for trial in results_base {
                        print!(",{:?}", trial);
                    }
                    print!(",{:+e},{:+e},{:+e},{:+e}", results_wl.0, results_wl.1, results_wl.2, results_wl.3);
                    println!();
                } else {
                    println!("Workload: {:?}", w);
                    println!("Threads: {}", threads);
                    println!("Keys: {}", keys);
                    println!("Elements: {}", elements);
                    println!("Capacity: {}", capacity.unwrap_or(keys));
                    println!("Zeroed: {}", !no_zero);
                    println!("Value Type: {:?}", value_type);
                    println!("Operator: {:?}", operator);
                    println!("Zipf: {}", zipf.unwrap_or(0.0));
                    println!("Heavy Hitter: {}", heavy_hitter.unwrap_or(0.0));
                    println!("Memory: {:+e}", mems.as_mut_slice().uqmedian(|v| u64::from_usize(*v).unwrap()).unwrap());
                    println!("Trials: {:?}", results_base);
                    println!("Initialization: {:+e}", results_wl.0);
                    println!("Stage 1: {:+e}", results_wl.1);
                    println!("Stage 2: {:+e}", results_wl.2);
                    println!("Materialization: {:+e}", results_wl.3);
                }
            };

            match value_type {
                ValueType::I16 => {
                    let mut kvs = generate_dataset::<i64, i16>(elements, keys, zipf, heavy_hitter);
                    for &w in workload.iter() {
                        let workload_closure = box_workload_closure::<i64, AtomicI16>(w, operator, threads, keys, capacity, !no_zero);
                        print_results(benchmark_harness(&mut kvs, workload_closure, iterations), w);
                    }
                }
                ValueType::I32 => {
                    let mut kvs = generate_dataset::<i64, i32>(elements, keys, zipf, heavy_hitter);
                    for &w in workload.iter() {
                        let workload_closure = box_workload_closure::<i64, AtomicI32>(w, operator, threads, keys, capacity, !no_zero);
                        print_results(benchmark_harness(&mut kvs, workload_closure, iterations), w);
                    }
                }
                ValueType::I64 => {
                    let mut kvs = generate_dataset::<i64, i64>(elements, keys, zipf, heavy_hitter);
                    for &w in workload.iter() {
                        let workload_closure = box_workload_closure::<i64, AtomicI64>(w, operator, threads, keys, capacity, !no_zero);
                        print_results(benchmark_harness(&mut kvs, workload_closure, iterations), w);
                    }
                }
            }
        },
        Commands::Profile { workload, workload_args, delay, control } => {
            let WorkloadArgs { threads, elements, keys, capacity, value_type, operator, zipf, heavy_hitter, no_zero, iterations } = workload_args;

            let elapsed;
            match value_type {
                ValueType::I16 => {
                    let setup_closure = || {
                        let (keys, values) = generate_dataset::<i64, i16>(elements, keys, zipf, heavy_hitter);
                        (keys, values)
                    };
                    let workload_closure = box_workload_closure::<i64, AtomicI16>(workload, operator, threads, keys, capacity, !no_zero);
                    elapsed = profile_harness(delay, control.to_str().unwrap(), setup_closure, workload_closure, iterations);
                }
                ValueType::I32 => {
                    let setup_closure = || {
                        let (keys, values) = generate_dataset::<i64, i32>(elements, keys, zipf, heavy_hitter);
                        (keys, values)
                    };
                    let workload_closure = box_workload_closure::<i64, AtomicI32>(workload, operator, threads, keys, capacity, !no_zero);
                    elapsed = profile_harness(delay, control.to_str().unwrap(), setup_closure, workload_closure, iterations);
                }
                ValueType::I64 => {
                    let setup_closure = || {
                        let (keys, values) = generate_dataset::<i64, i64>(elements, keys, zipf, heavy_hitter);
                        (keys, values)
                    };
                    let workload_closure = box_workload_closure::<i64, AtomicI64>(workload, operator, threads, keys, capacity, !no_zero);
                    elapsed = profile_harness(delay, control.to_str().unwrap(), setup_closure, workload_closure, iterations);
                }
            }

            println!("Elapsed time: {:+e}", elapsed);
        },
    }
}
