use std::{fs, thread};
use std::sync::atomic::AtomicI64;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use itertools::Itertools;
use peak_alloc::PeakAlloc;
use rand::prelude::*;
use rand_distr::{Uniform, Zipf};
use rayon::prelude::*;

use common::FuzzyCounter;
use ticket::{Ticketer, KeyedFolkloreTicketer, KeyedTicketer};
use partial_aggregate_update::*;
use partitioned_aggregate::*;

const CHUNK_SIZE: usize = 1 << 16; // = 65536, needs to be set decently high, actually.
const TIMEOUT: f64 = 600f64;

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

pub fn generate_dataset(elements: usize, groups: usize, zipf: Option<f64>, heavy_hitter: Option<f64>) -> (Vec<i64>, Vec<i64>) {
    let mut keys = vec![0; elements];
    let mut values = vec![0; elements];

    let min_key: i64 = 0;
    let max_key: i64 = groups as i64;
    let min_value: i64 = 0;
    let max_value: i64 = 10_000;

    let mut rng = SmallRng::seed_from_u64(42);
    let target_distr = Uniform::new::<i64, _>(min_key, max_key).unwrap();
    let target = target_distr.sample(&mut rng);

    keys.par_chunks_mut(CHUNK_SIZE)
        .zip(values.par_chunks_mut(CHUNK_SIZE))
        .enumerate()
        .for_each(|(chunk_num, (keys, values))| {
            let mut rng = SmallRng::seed_from_u64(chunk_num as u64);

            if let Some(s) = zipf {
                let key_distr = Zipf::new((max_key - min_key) as f64, s).unwrap();
                let value_distr = Zipf::new((max_value - min_value) as f64, s).unwrap();

                keys.iter_mut().for_each(|k| *k = key_distr.sample(&mut rng) as i64 - 1);
                values.iter_mut().for_each(|v| *v = value_distr.sample(&mut rng) as i64 - 1);
            } else if let Some(p) = heavy_hitter {
                let key_distr = Uniform::new::<i64, _>(min_key, max_key).unwrap();
                let value_distr = Uniform::new::<i64, _>(min_value, max_value).unwrap();

                keys.iter_mut().for_each(|k| {
                    if rng.random_bool(p) {
                        *k = target.clone();
                    } else {
                        *k = key_distr.sample(&mut rng)
                    }
                });
                values.iter_mut().for_each(|v| *v = value_distr.sample(&mut rng));
            } else {
                let key_distr = Uniform::new::<i64, _>(min_key, max_key).unwrap();
                let value_distr = Uniform::new::<i64, _>(min_value, max_value).unwrap();

                keys.iter_mut().for_each(|k| *k = key_distr.sample(&mut rng));
                values.iter_mut().for_each(|v| *v = value_distr.sample(&mut rng));
            }
        });

    (keys, values)
}

pub fn ticketing_workload<H: Ticketer<i64>>(threads: usize, num_keys: usize, keys: &Vec<i64>, _values: &Vec<i64>, capacity: Option<usize>) {
    let capacity = capacity.unwrap_or(num_keys);
    let hm: H = H::with_capacity_and_threads(capacity, threads);
    let tasks = RwLock::new(keys.chunks(CHUNK_SIZE));

    thread::scope(|s| {
        (0..threads)
            .map(|_| s.spawn(|| {
                let mut o = vec![0usize; CHUNK_SIZE]; // Local output is important to avoid cache thrashing.

                loop {
                    let task = tasks.write().unwrap().next();
                    match task {
                        Some(k) => {
                            hm.ticket(k, &mut o);
                        },
                        None => break,
                    }
                }
            }))
            .collect_vec() // Force iterator evaluation.
            .into_iter()
            .for_each(|jh| jh.join().expect("Thread error") );
    });

    // Note we do materialize because in an actually ticketing workload, we simply just store key
    // as we aggregate in ticket order. To materialize is a trivial return of this array not a full
    // materialization of the hash table.
}

pub fn update_workload<U: Updater<i64>>(threads: usize, num_keys: usize, keys: &Vec<i64>, values: &Vec<i64>, capacity: Option<usize>) -> (f64, f64, f64) {
    // Assume keys are integers counting up from 0 so they can be used as ticketing values (essentially a PHF) to isolate the partial-aggregate-update.
    let tasks = RwLock::new(
        keys.chunks(CHUNK_SIZE)
            .zip(values.chunks(CHUNK_SIZE))
    );
    let capacity = capacity.unwrap_or(num_keys);
    let time = Instant::now();
    let updater: U = U::with_capacity_and_threads(capacity, threads);
    let init_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
    thread::scope(|s| {
        (0..threads)
            .map(|_| s.spawn(|| {
                let mut t = vec![0usize; CHUNK_SIZE];

                loop {
                    let task = tasks.write().unwrap().next();
                    match task {
                        Some((k, v)) => {
                            k.iter().enumerate().for_each(|(idx, k)| t[idx] = *k as usize);
                            updater.update_vec(&t[0..k.len()], v);
                        },
                        None => break,
                    }
                }
            }))
            .collect_vec() // Force iterator evaluation.
            .into_iter()
            .for_each(|jh| {
                jh.join().expect("Thread error");
            });
    });
    let agg_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
    let _aggs_out = updater.into_vec();
    let mat_time = time.elapsed().as_secs_f64();

    (init_time, agg_time, mat_time)
}

pub fn end_to_end_workload<T: KeyedTicketer<i64>, U: Updater<i64>, const BREAKDOWN: bool>(
    threads: usize, num_keys: usize, keys: &Vec<i64>, values: &Vec<i64>, capacity: Option<usize>
) -> Option<(f64, Vec<Vec<f64>>, Vec<Vec<f64>>, f64, f64)> {
    let tasks = RwLock::new(
        keys.chunks(CHUNK_SIZE)
            .zip(values.chunks(CHUNK_SIZE))
    );
    let capacity = capacity.unwrap_or(num_keys) + 1; // Hacky + 1 because our folklore map reserves ticket 0 for default key.

    let time = Instant::now();
    let hm = KeyedFolkloreTicketer::<AtomicI64>::with_capacity_and_threads(capacity, threads);
    let updater: U = U::with_capacity_and_threads(capacity + threads * FuzzyCounter::DEFAULT_STEP_SIZE, threads);
    let init_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
    let mut ticket_times: Vec<Vec<f64>> = Vec::new();
    let mut agg_times: Vec<Vec<f64>> = Vec::new();
    thread::scope(|s| {
        (0..threads)
            .map(|_| s.spawn(|| {
                let mut o = vec![0usize; CHUNK_SIZE]; // Local output is important to avoid cache thrashing.
                let mut ticket_times = Vec::new();
                let mut agg_times = Vec::new();

                loop {
                    let task = tasks.write().unwrap().next();
                    let mut time = Instant::now();
                    match task {
                        Some((k, v)) => {
                            hm.ticket(k, &mut o);
                            if BREAKDOWN { ticket_times.push(time.elapsed().as_secs_f64()); time = Instant::now(); }
                            updater.update_vec(&mut o, v);
                            if BREAKDOWN { agg_times.push(time.elapsed().as_secs_f64()); }
                        },
                        None => break,
                    }
                }

                if BREAKDOWN { Some((ticket_times, agg_times)) } else { None }
            }))
            .collect_vec() // Force iterator evaluation.
            .into_iter()
            .for_each(|jh| {
                let res = jh.join().expect("Thread error");
                if let Some((ticket, agg)) = res {
                    ticket_times.push(ticket);
                    agg_times.push(agg);
                }
            });
    });
    let work_time = time.elapsed().as_secs_f64();

    let time = Instant::now();

    // Materializes keys and aggregates. All vectors are shifted to be dense.
    let (_keys_out, finalizer) = hm.into_keys();
    let mut aggs_out = updater.into_vec();
    let len = finalizer.reorder_slice(aggs_out.as_mut_slice());
    aggs_out.truncate(len);

    let mat_time = time.elapsed().as_secs_f64();

    if BREAKDOWN {
        Some((init_time, ticket_times, agg_times, work_time, mat_time))
    } else {
        None
    }
}

pub fn partitioned_end_to_end_workload<A: PartitionedAggregator<i64, i64>>(threads: usize, num_keys: usize, keys: &Vec<i64>, values: &Vec<i64>, capacity: Option<usize>) {
    let tasks = RwLock::new(
        keys.chunks(CHUNK_SIZE)
            .zip(values.chunks(CHUNK_SIZE))
    );
    let capacity = capacity.unwrap_or(num_keys);

    let aggregator: A = A::with_capacity_and_threads(capacity, threads);
    thread::scope(|s| {
        (0..threads)
            .map(|_| s.spawn(|| {
                loop {
                    let task = tasks.write().unwrap().next();
                    match task {
                        Some((k, v)) => {
                            aggregator.aggregate_vec(&k, &v);
                        },
                        None => break,
                    }
                }
                aggregator.finalize_thread()
            }))
            .collect_vec() // Force iterator evaluation.
            .into_iter()
            .for_each(|jh| jh.join().expect("Thread error"));
    });
    let finalizer = aggregator.into_finalizer();

    thread::scope(|s| {
        (0..threads)
            .map(|_| s.spawn(|| {
                finalizer.finalize_thread();
            }))
            .collect_vec() // Force iterator evaluation.
            .into_iter()
            .for_each(|jh| jh.join().expect("Thread error"));
    });
    finalizer.into_vec();
}

pub fn benchmark_harness<I, O, W>(input: &mut I, workload: W, iterations: usize) -> (Vec<f64>, Vec<f64>, Vec<O>)
where W: Fn(&mut I) -> O,
{
    let mut times: Vec<f64> = vec![0f64; iterations];
    let mut mems = vec![0f64; iterations];
    let mut outputs = Vec::new();
    let timeout = Instant::now();


    // Warmup.
    for _ in 0..iterations.div_ceil(4) {
        workload(input);
        thread::sleep(Duration::new(1, 0));

        if timeout.elapsed().as_secs_f64() > TIMEOUT / 4f64 {
            break;
        }
    }

    // Workload.
    let timeout = Instant::now();
    let mut time = Instant::now();
    for i in 0..iterations {
        let init_mem = PEAK_ALLOC.current_usage_as_gb() as f64;
        let o = workload(input);
        times[i] = time.elapsed().as_secs_f64();
        outputs.push(o);
        let peak_mem = PEAK_ALLOC.peak_usage_as_gb() as f64;
        mems[i] = peak_mem - init_mem;
        PEAK_ALLOC.reset_peak_usage();
        time = Instant::now();
        if timeout.elapsed().as_secs_f64() > TIMEOUT {
            break;
        }
    }


    (times, mems, outputs)
}

pub fn profile_harness<I, O, S, W>(pipe: &str, setup: S, workload: W, iterations: usize)
where S: Fn() -> I,
      W: Fn(&mut I) -> O,
{
    fs::write(pipe, "disable".as_bytes()).unwrap();
    let mut input: I = setup();
    let timeout = Instant::now();

    // Warmup.
    for _ in 0..iterations.div_ceil(4) {
        workload(&mut input);
        if timeout.elapsed().as_secs_f64() > TIMEOUT / 4f64 {
            break;
        }
    }

    // Workload.
    let timeout = Instant::now();
    fs::write(pipe, "enable".as_bytes()).unwrap();
    for _ in 0..iterations {
        workload(&mut input);
        if timeout.elapsed().as_secs_f64() > TIMEOUT {
            break;
        }
    }
    fs::write(pipe, "disable".as_bytes()).unwrap();
}
