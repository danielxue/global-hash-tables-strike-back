use std::path::PathBuf;
use std::sync::atomic::AtomicI64;

use clap::{Parser, Subcommand, ValueEnum};
use clap_derive::Args;
use itertools::Itertools;
use medians::Medianf64;

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

        #[arg(long)]
        breakdown: bool,
    },
    Profile {
        #[arg(short, long, value_enum)]
        workload: Workload,

        #[command(flatten)]
        workload_args: WorkloadArgs,

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

    #[arg(long, group = "distribution")]
    zipf: Option<f64>,

    #[arg(long, group = "distribution")]
    heavy_hitter: Option<f64>,

    #[arg(short, long, default_value_t = 5)]
    iterations: usize,
}

#[derive(ValueEnum, Debug, Clone)]
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

    AdHocResizingPartitionedE2E,
    LinearProbingPartitionedE2E,
    NoSpillPartitionedE2E,
    PaginatedPartitionedE2E,
    PartitionedE2E,
}

enum Breakdown {
    Ticketing(()),
    Update((f64, f64, f64)),
    E2EAggregation(Option<(f64, Vec<Vec<f64>>, Vec<Vec<f64>>, f64, f64)>),
    E2EPartitionedAggregation(()),
}

type DefaultTicketer = KeyedFolkloreTicketer<AtomicI64>;

fn box_ticketing_workload<T: Ticketer<i64>>(threads: usize, keys: usize, capacity: Option<usize>)
                                            -> Box<dyn Fn(&mut (Vec<i64>, Vec<i64>)) -> Breakdown> {
    Box::new(move |kv| {
        Breakdown::Ticketing(ticketing_workload::<T>(threads, keys, &kv.0, &kv.1, capacity))
    })
}

fn box_update_workload<U: Updater<i64>>(threads: usize, keys: usize, capacity: Option<usize>)
    -> Box<dyn Fn(&mut (Vec<i64>, Vec<i64>)) -> Breakdown> {
    Box::new(move |kv| {
        Breakdown::Update(update_workload::<U>(threads, keys, &kv.0, &kv.1, capacity))
    })
}

fn box_e2e_workload<T: KeyedTicketer<i64>, U: Updater<i64>>(threads: usize, keys: usize, capacity: Option<usize>, breakdown: bool)
    -> Box<dyn Fn(&mut (Vec<i64>, Vec<i64>)) -> Breakdown>{
    if breakdown {
        Box::new(move |kv| {
            Breakdown::E2EAggregation(end_to_end_workload::<T, U, true>(threads, keys, &kv.0, &kv.1, capacity))
        })
    } else {
        Box::new(move |kv| {
            Breakdown::E2EAggregation(end_to_end_workload::<T, U, false>(threads, keys, &kv.0, &kv.1, capacity))
        })
    }
}

fn box_e2e_partitioned_workload<A: PartitionedAggregator<i64, i64>>(threads: usize, keys: usize, capacity: Option<usize>)
                                                                    -> Box<dyn Fn(&mut (Vec<i64>, Vec<i64>)) -> Breakdown> {
    Box::new(move |kv| {
        Breakdown::E2EPartitionedAggregation(partitioned_end_to_end_workload::<A>(threads, keys, &kv.0, &kv.1, capacity))
    })
}

fn box_workload_closure(workload: Workload, threads: usize, keys: usize, capacity: Option<usize>, breakdown: bool)
                        -> Box<dyn Fn(&mut (Vec<i64>, Vec<i64>)) -> Breakdown> {
    match workload.clone() {
        Workload::CuckooMap
        | Workload::DashMap
        | Workload::FolkloreMap
        | Workload::FolkloreUnfuzzyMap
        | Workload::IcebergMap
        | Workload::LeapMap
        | Workload::GlobalLockingMap
        | Workload::OnceLockMap => {
            match workload {
                Workload::CuckooMap => box_ticketing_workload::<CuckooTicketer<i64>>(threads, keys, capacity),
                Workload::DashMap => box_ticketing_workload::<DashTicketer<i64>>(threads, keys, capacity),
                Workload::FolkloreMap => box_ticketing_workload::<FolkloreTicketer<AtomicI64>>(threads, keys, capacity),
                Workload::FolkloreUnfuzzyMap => box_ticketing_workload::<FolkloreUnfuzzyTicketer<AtomicI64>>(threads, keys, capacity),
                Workload::IcebergMap => box_ticketing_workload::<IcebergTicketer<i64>>(threads, keys, capacity),
                Workload::LeapMap => box_ticketing_workload::<LeapTicketer<i64>>(threads, keys, capacity),
                Workload::GlobalLockingMap => box_ticketing_workload::<GlobalLockingTicketer<i64>>(threads, keys, capacity),
                Workload::OnceLockMap => box_ticketing_workload::<OnceLockHashMap<i64>>(threads, keys, capacity),
                _ => panic!("Error parsing workload: {:?}", workload),
            }
        },
        Workload::AtomicPau
        | Workload::LockingPau
        | Workload::GlobalLockingPau
        | Workload::ThreadLocalPau => {
            match workload {
                Workload::AtomicPau => box_update_workload::<atomic_pau::CountUpdater<i64>>(threads, keys, capacity),
                Workload::LockingPau => box_update_workload::<locking_agg::CountUpdater<i64>>(threads, keys, capacity),
                Workload::GlobalLockingPau => box_update_workload::<global_locking_agg::CountUpdater<i64>>(threads, keys, capacity),
                Workload::ThreadLocalPau => box_update_workload::<thread_local_agg::CountUpdater<i64>>(threads, keys, capacity),
                _ => panic!("Error parsing workload: {:?}", workload),
            }
        },
        Workload::AtomicE2E
        | Workload::LockingE2E
        | Workload::GlobalLockingE2E
        | Workload::ThreadLocalE2E => {
            match workload {
                Workload::AtomicE2E => box_e2e_workload::<DefaultTicketer, atomic_pau::CountUpdater<i64>>(threads, keys, capacity, breakdown),
                Workload::LockingE2E => box_e2e_workload::<DefaultTicketer, locking_agg::CountUpdater<i64>>(threads, keys, capacity, breakdown),
                Workload::GlobalLockingE2E => box_e2e_workload::<DefaultTicketer, global_locking_agg::CountUpdater<i64>>(threads, keys, capacity, breakdown),
                Workload::ThreadLocalE2E => box_e2e_workload::<DefaultTicketer, thread_local_agg::CountUpdater<i64>>(threads, keys, capacity, breakdown),
                _ => panic!("Error parsing workload: {:?}", workload),
            }
        },
        Workload::AdHocResizingPartitionedE2E
        | Workload::LinearProbingPartitionedE2E
        | Workload::NoSpillPartitionedE2E
        | Workload::PaginatedPartitionedE2E
        | Workload::PartitionedE2E => {
            match workload {
                Workload::AdHocResizingPartitionedE2E => box_e2e_partitioned_workload::<ad_hoc_resizing_basic_partitioned_agg::CountAgg<i64, i64>>(threads, keys, capacity),
                Workload::LinearProbingPartitionedE2E => box_e2e_partitioned_workload::<linear_probing_partitioned_agg::CountAgg<i64, i64>>(threads, keys, capacity),
                Workload::NoSpillPartitionedE2E => box_e2e_partitioned_workload::<no_spill_partitioned_agg::CountAgg<i64, i64>>(threads, keys, capacity),
                Workload::PaginatedPartitionedE2E => box_e2e_partitioned_workload::<paginated_partitioned_agg::CountAgg<i64, i64>>(threads, keys, capacity),
                Workload::PartitionedE2E => box_e2e_partitioned_workload::<basic_partitioned_agg::CountAgg<i64, i64>>(threads, keys, capacity),
                _ => panic!("Error parsing workload: {:?}", workload),
            }
        },
    }
}

fn process_breakdown(breakdown: Vec<Breakdown>, enabled: bool) -> Option<(f64, f64, f64, f64)> {
    if !enabled {
        return None;
    }

    match breakdown[0] {
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

            let mut initalization_times = Vec::new();
            let mut aggregation_times = Vec::new();
            let mut materialization_times = Vec::new();

            res.into_iter()
                .for_each(|(initalization, aggregation, materialization)| {
                    initalization_times.push(initalization);
                    aggregation_times.push(aggregation);
                    materialization_times.push(materialization);
                });

            Some((
                initalization_times.as_mut_slice().medf_unchecked(),
                0f64,
                aggregation_times.as_mut_slice().medf_unchecked(),
                materialization_times.as_mut_slice().medf_unchecked(),
            ))
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

            let mut initalization_times = Vec::new();
            let mut ticketing_times = Vec::new();
            let mut aggregation_times = Vec::new();
            let mut materialization_times = Vec::new();

            res.into_iter()
                .map(|trial| trial.unwrap())
                .map(|(init, s1, s2, s12, mat)| {
                    let s1 = s1.into_iter().flat_map(|thread_counts| thread_counts.into_iter()).sum::<f64>();
                    let s2 = s2.into_iter().flat_map(|thread_counts| thread_counts.into_iter()).sum::<f64>();
                    (init, (s1 / (s1 + s2)) * s12, (s2 / (s1 + s2)) * s12, mat)
                })
                .for_each(|(initialization, ticketing, aggregation, materialization)| {
                    initalization_times.push(initialization);
                    ticketing_times.push(ticketing);
                    aggregation_times.push(aggregation);
                    materialization_times.push(materialization);
                });

            Some((
                initalization_times.as_mut_slice().medf_unchecked(),
                ticketing_times.as_mut_slice().medf_unchecked(),
                aggregation_times.as_mut_slice().medf_unchecked(),
                materialization_times.as_mut_slice().medf_unchecked(),
            ))
        },
        _ => None,
    }
}

fn main() {
    let args = Args::parse();

    match args.command {
        Commands::Bench { workload, workload_args, table, breakdown } => {
            let WorkloadArgs { threads, elements, keys, capacity, zipf, heavy_hitter, iterations } = workload_args;
            let mut kvs = generate_dataset(elements, keys, zipf, heavy_hitter);

            for w in workload {
                let workload_closure = box_workload_closure(w.clone(), threads, keys, capacity, breakdown);
                let (results_base, mut mems, breakdown_results) = benchmark_harness(&mut kvs, workload_closure, iterations);
                let results_wl: Option<(f64, f64, f64, f64)> = process_breakdown(breakdown_results, breakdown);

                if table {
                    print!(
                        "{:?},{},{},{},{},{},{},{:+e}",
                        w,
                        threads,
                        keys,
                        elements,
                        capacity.unwrap_or(keys),
                        zipf.unwrap_or(0.0),
                        heavy_hitter.unwrap_or(0.0),
                        mems.as_mut_slice().medf_unchecked(),
                    );
                    for trial in results_base {
                        print!(",{:?}", trial);
                    }
                    if let Some(results_wl) = results_wl {
                        print!(",{:+e},{:+e},{:+e},{:+e}", results_wl.0, results_wl.1, results_wl.2, results_wl.3);
                    }
                    println!();
                } else {
                    println!("Workload: {:?}", w);
                    println!("Threads: {}", threads);
                    println!("Keys: {}", keys);
                    println!("Elements: {}", elements);
                    println!("Capacity: {}", capacity.unwrap_or(keys));
                    println!("Zipf: {}", zipf.unwrap_or(0.0));
                    println!("Heavy Hitter: {}", heavy_hitter.unwrap_or(0.0));
                    println!("Memory: {:+e}", mems.as_mut_slice().medf_unchecked());
                    println!("Trials: {:?}", results_base);
                    if let Some(results_wl) = results_wl {
                        println!("Initialization: {:+e}", results_wl.0);
                        println!("Stage 1: {:+e}", results_wl.1);
                        println!("Stage 2: {:+e}", results_wl.2);
                        println!("Materialization: {:+e}", results_wl.3);
                    }
                }
            }
        },
        Commands::Profile { workload, workload_args, control } => {
            let WorkloadArgs { threads, elements, keys, capacity, zipf, heavy_hitter, iterations } = workload_args;
            let setup_closure = || {
                let (keys, values) = generate_dataset(elements, keys, zipf, heavy_hitter);
                (keys, values)
            };

            let workload_closure = box_workload_closure(workload, threads, keys, capacity, false);
            profile_harness(control.to_str().unwrap(), setup_closure, workload_closure, iterations);
        },
    }
}
