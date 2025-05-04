#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. ${script_dir}/run_workload.sh

output_file=${script_dir}/../data/scaling_experiment.csv
echo "workload,threads,keys,elements,capacity,zipf,heavy hitter,memory,t1,t2,t3,t4,t5,t6,t7,t8,t9,initialization,ticketing,update,materialization" > $output_file

for thread in "${threads[@]}"; do
    ## Low cardinality (1000 / 100 million elements), ~100% lookup/~0% insertion.
    # Uniform data distribution.
    bench $output_file $ticketing $update $e2e -t $thread -k 1000 -e 100000000 --breakdown

    # Skewed data distribution.
    bench $output_file $e2e -t $thread -k 1000 -e 100000000 --zipf=0.8 --breakdown # Only run end-to-end to produce Table 2, not shown in Figure 6.

    # Heavy hitter data distribution.
    bench $output_file $e2e -t $thread -k 1000 -e 100000000 --heavy-hitter=0.5 --breakdown # Only run end-to-end to produce Table 2, not shown in Figure 6.

    ## High cardinality (10 million keys / 100 million elements), ~90% lookup/~10% insertion.
    # Uniform data distribution.
    bench $output_file $ticketing $update $e2e  -t $thread -k 10000000 -e 100000000 --breakdown

    # Skewed data distribution.
    bench $output_file $ticketing $update $e2e  -t $thread -k 10000000 -e 100000000 --zipf=0.8 --breakdown

    # Heavy hitter data distribution.
    bench $output_file $ticketing $update $e2e  -t $thread -k 10000000 -e 100000000 --heavy-hitter=0.5 --breakdown

    ## Unique keys (100 million keys / 100 million elements), ~0% lookup/~100% insertion.
    # Uniform data distribution.
    bench $output_file $ticketing $update $e2e -t $thread -k 100000000 -e 100000000 --breakdown

    # Skewed data distribution.
    bench $output_file $e2e -t $thread -k 100000000 -e 100000000 --zipf=0.8  --breakdown # Only run end-to-end to produce Table 2, not shown in Figure 6.

    # Heavy hitter data distribution.
    bench $output_file  $e2e -t $thread -k 100000000 -e 100000000 --heavy-hitter=0.5 --breakdown # Only run end-to-end to produce Table 2, not shown in Figure 6.
done
