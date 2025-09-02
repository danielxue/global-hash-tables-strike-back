use std::{fs, thread};
use std::cmp::min;
use std::sync::RwLock;
use std::time::Instant;

use itertools::Itertools;
use rand::prelude::*;
use rand_distr::{Uniform, Zipf};
use rayon::prelude::*;

use common::FuzzyCounter;
use ticket::{Ticketer, KeyedTicketer};
use partial_aggregate_update::*;
use partitioned_aggregate::*;

use std::alloc::System;
use peakmem_alloc::*;
#[global_allocator]
static PEAK_ALLOC: &PeakMemAlloc<System> = &INSTRUMENTED_SYSTEM;

const CHUNK_SIZE: usize = 1 << 16; // = 65536, needs to be set decently high, actually.
const TIMEOUT: f64 = 600f64;

pub fn generate_dataset<K, V>(
    elements: usize,
    groups: usize,
    zipf: Option<f64>,
    heavy_hitter: Option<f64>
) -> (Vec<K>, Vec<V>)
where K: Send + Sync + Clone + Default + TryFrom<i64>,
      V: Send + Sync + Clone + Default + TryFrom<i64>,
{
    let mut keys = vec![K::default(); elements];
    let mut values = vec![V::default(); elements];

    let min_key = 0;
    let max_key = groups as i64;
    let min_value = -100;
    let max_value = 100;

    let mut rng = SmallRng::seed_from_u64(42);
    let target_distr = Uniform::new::<i64, _>(min_key, max_key).unwrap();
    let target = K::try_from(target_distr.sample(&mut rng)).unwrap_or_default();

    let pool = rayon::ThreadPoolBuilder::new().num_threads(
        thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    ).build().unwrap();

    pool.install(|| {
        keys.par_chunks_mut(CHUNK_SIZE)
            .zip(values.par_chunks_mut(CHUNK_SIZE))
            .enumerate()
            .for_each(|(chunk_num, (keys, values))| {
                let mut rng = SmallRng::seed_from_u64(chunk_num as u64);

                if let Some(s) = zipf {
                    let key_distr = Zipf::new((max_key - min_key) as f64, s).unwrap();
                    let value_distr = Zipf::new((max_value - min_value) as f64, s).unwrap();

                    keys.iter_mut().for_each(|k| *k = K::try_from(key_distr.sample(&mut rng) as i64 - 1).unwrap_or_default());
                    values.iter_mut().for_each(|v| *v = V::try_from(value_distr.sample(&mut rng) as i64 - 1).unwrap_or_default());
                } else if let Some(p) = heavy_hitter {
                    let key_distr = Uniform::new::<i64, _>(min_key, max_key).unwrap();
                    let value_distr = Uniform::new::<i64, _>(min_value, max_value).unwrap();

                    keys.iter_mut().for_each(|k| {
                        if rng.random_bool(p) {
                            *k = target.clone();
                        } else {
                            *k = K::try_from(key_distr.sample(&mut rng)).unwrap_or_default();
                        }
                    });
                    values.iter_mut().for_each(|v| *v = V::try_from(value_distr.sample(&mut rng)).unwrap_or_default());
                } else {
                    let value_distr = Uniform::new::<i64, _>(min_value, max_value).unwrap();
                    values.iter_mut().for_each(|v| *v = V::try_from(value_distr.sample(&mut rng)).unwrap_or_default());
                }
            });
    });

    // Force perfectly uniform distribution for uniform keys.
    if zipf.is_none() && heavy_hitter.is_none() {
        keys.iter_mut().enumerate().for_each(|(i, k)| *k = K::try_from(i as i64 % max_key).unwrap_or_default());
        keys.shuffle(&mut rng);
    }

    (keys, values)
}

pub fn ticketing_workload<H: Ticketer<i64>, V>(
    threads: usize,
    num_keys: usize,
    keys: &Vec<i64>,
    _values: &Vec<V>,
    capacity: Option<usize>,
    zeroed: bool,
) -> (f64, f64, f64) {
    let capacity = capacity.unwrap_or(min(num_keys, keys.len()));

    let time = Instant::now();
    let hm: H = if zeroed { H::with_capacity_and_threads_zeroed(capacity, threads) } else { H::with_capacity_and_threads(capacity, threads) };
    let tasks = RwLock::new(keys.chunks(CHUNK_SIZE));
    let init_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
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
    let ticketing_time = time.elapsed().as_secs_f64();

    // Note we do not materialize because in an actual ticketing workload, we duplicate keys and
    // store in ticket order. To materialize is a trivial return of this array not a full
    // materialization of the hash table. We do include deallocation to match the fact that
    // other benchmarks consume the hash table in this step.
    let time = Instant::now();
    drop(hm);
    let mat_time = time.elapsed().as_secs_f64();

    (init_time, ticketing_time, mat_time)
}

pub fn update_workload<U: Updater<V>, V: Send + Sync>(
    threads: usize,
    num_keys: usize,
    keys: &Vec<i64>,
    values: &Vec<V>,
    capacity: Option<usize>,
    zeroed: bool,
) -> (f64, f64, f64) {
    let tasks = RwLock::new(
        keys.chunks(CHUNK_SIZE)
            .zip(values.chunks(CHUNK_SIZE))
    );    let capacity = capacity.unwrap_or(min(num_keys, keys.len()));

    let time = Instant::now();
    let updater: U = if zeroed { U::with_capacity_and_threads_zeroed(capacity + threads * FuzzyCounter::DEFAULT_STEP_SIZE, threads) } else { U::with_capacity_and_threads(capacity + threads * FuzzyCounter::DEFAULT_STEP_SIZE, threads) };
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
                            k.iter().enumerate().for_each(|(idx, k)| t[idx] = (*k as usize) % capacity);
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
    let pau_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
    let _aggs_out = updater.into_vec();
    let mat_time = time.elapsed().as_secs_f64();

    (init_time, pau_time, mat_time)
}

pub fn end_to_end_workload<T: KeyedTicketer<i64>, U: Updater<V>, V: Send + Sync>(
    threads: usize,
    num_keys: usize,
    keys: &Vec<i64>,
    values: &Vec<V>,
    capacity: Option<usize>,
    zeroed: bool,
) -> Option<(f64, Vec<Vec<f64>>, Vec<Vec<f64>>, f64, f64)> {
    let tasks = RwLock::new(
        keys.chunks(CHUNK_SIZE)
            .zip(values.chunks(CHUNK_SIZE))
    );
    let capacity = capacity.unwrap_or(min(num_keys, keys.len()));

    let time = Instant::now();
    let hm = if zeroed { T::with_capacity_and_threads_zeroed(capacity, threads) } else { T::with_capacity_and_threads(capacity, threads) };
    let updater: U = if zeroed { U::with_capacity_and_threads_zeroed(capacity + threads * FuzzyCounter::DEFAULT_STEP_SIZE, threads) } else { U::with_capacity_and_threads(capacity + threads * FuzzyCounter::DEFAULT_STEP_SIZE, threads) };
    let init_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
    let mut ticket_times: Vec<Vec<f64>> = Vec::new();
    let mut pau_times: Vec<Vec<f64>> = Vec::new();
    thread::scope(|s| {
        (0..threads)
            .map(|_| s.spawn(|| {
                let mut o = vec![0usize; CHUNK_SIZE]; // Local output is important to avoid cache thrashing.
                let mut ticket_times = Vec::new();
                let mut pau_times = Vec::new();

                loop {
                    let task = tasks.write().unwrap().next();
                    let mut time = Instant::now();
                    match task {
                        Some((k, v)) => {
                            hm.ticket(k, &mut o);
                            ticket_times.push(time.elapsed().as_secs_f64());
                            time = Instant::now();

                            updater.update_vec(&mut o, v);
                            pau_times.push(time.elapsed().as_secs_f64());
                        },
                        None => break,
                    }
                }

                Some((ticket_times, pau_times))
            }))
            .collect_vec() // Force iterator evaluation.
            .into_iter()
            .for_each(|jh| {
                let res = jh.join().expect("Thread error");
                if let Some((ticket, agg)) = res {
                    ticket_times.push(ticket);
                    pau_times.push(agg);
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

    Some((init_time, ticket_times, pau_times, work_time, mat_time))
}

pub fn partitioned_end_to_end_workload<A: PartitionedAggregator<i64, V>, V: Send + Sync>(
    threads: usize,
    num_keys: usize,
    keys: &Vec<i64>,
    values: &Vec<V>,
    capacity: Option<usize>,
    zeroed: bool,
) -> (f64, f64, f64, f64) {
    let tasks = RwLock::new(
        keys.chunks(CHUNK_SIZE)
            .zip(values.chunks(CHUNK_SIZE))
    );
    let capacity = capacity.unwrap_or(min(num_keys, keys.len()));

    let time = Instant::now();
    let aggregator: A = if zeroed { A::with_capacity_and_threads_zeroed(capacity, threads) } else { A::with_capacity_and_threads(capacity, threads) };
    let init_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
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
    let pre_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
    thread::scope(|s| {
        (0..threads)
            .map(|_| s.spawn(|| {
                finalizer.finalize_thread();
            }))
            .collect_vec() // Force iterator evaluation.
            .into_iter()
            .for_each(|jh| jh.join().expect("Thread error"));
    });
    let local_time = time.elapsed().as_secs_f64();

    let time = Instant::now();
    finalizer.into_vec();
    let mat_time = time.elapsed().as_secs_f64();

    (init_time, pre_time, local_time, mat_time)
}

pub fn benchmark_harness<I, O, W>(input: &mut I, workload: W, iterations: usize) -> (Vec<f64>, Vec<usize>, Vec<O>)
where W: Fn(&mut I) -> O,
{
    let mut times: Vec<f64> = vec![0f64; iterations];
    let mut mems = vec![0usize; iterations];
    let mut outputs = Vec::new();
    let timeout = Instant::now();

    // Warmup.
    for _ in 0..iterations.div_ceil(4) {
        workload(input);
        if timeout.elapsed().as_secs_f64() > TIMEOUT / 4f64 {
            break;
        }
    }

    // Workload.
    let timeout = Instant::now();
    for i in 0..iterations {
        PEAK_ALLOC.reset_peak_memory();
        let time = Instant::now();
        let o = workload(input);
        times[i] = time.elapsed().as_secs_f64();
        mems[i] = PEAK_ALLOC.get_peak_memory();
        outputs.push(o);
        if timeout.elapsed().as_secs_f64() > TIMEOUT {
            break;
        }
    }

    (times, mems, outputs)
}

pub fn profile_harness<I, O, S, W>(delay: usize, pipe: &str, setup: S, workload: W, iterations: usize) -> f64
where S: Fn() -> I,
      W: Fn(&mut I) -> O,
{
    fs::write(pipe, "disable".as_bytes()).unwrap();
    let delay_time = Instant::now();
    let mut input: I = setup();
    let timeout = Instant::now();

    // Warmup.
    for _ in 0..iterations.div_ceil(4) {
        workload(&mut input);
        if timeout.elapsed().as_secs_f64() > TIMEOUT / 4f64 {
            break;
        }
    }

    while delay_time.elapsed().as_millis() < delay as u128 {
        continue;
    }

    // Workload.
    let timeout = Instant::now();
    println!("Setup time: {:+e}", delay_time.elapsed().as_secs_f64());
    fs::write(pipe, "enable".as_bytes()).unwrap();
    for _ in 0..iterations {
        workload(&mut input);
        if timeout.elapsed().as_secs_f64() > TIMEOUT {
            break;
        }
    }
    fs::write(pipe, "disable".as_bytes()).unwrap();

    timeout.elapsed().as_secs_f64()
}
