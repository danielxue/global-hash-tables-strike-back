#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. ${script_dir}/run_workload.sh

output_file=${script_dir}/../data/resizing_experiment.csv
echo "workload,threads,keys,elements,capacity,zipf,heavy hitter,memory,t1,t2,t3,t4,t5,t6,t7,t8,t9,initialization,ticketing,update,materialization" > $output_file

for thread in "${threads[@]}"; do
    ## High cardinality (10 million keys / 100 million elements), ~90% lookup/~10% insertion.
    # No resizing.
    bench $output_file -w atomic-e2e -w thread-local-e2e -w partitioned-e2e -t $thread -k 10000000 -e 100000000 --breakdown

    # Half capacity (should resize once).
    bench $output_file -w atomic-e2e -w thread-local-e2e -w partitioned-e2e -t $thread -k 10000000 -e 100000000 --capacity 5000000 --breakdown

    ## Unique keys (100 million keys / 100 million elements), ~0% lookup/~100% insertion.
    # No resizing.
    bench $output_file -w atomic-e2e -w thread-local-e2e -w partitioned-e2e -t $thread -k 100000000 -e 100000000 --breakdown

    # Half capacity (should resize once).
    bench $output_file -w atomic-e2e -w thread-local-e2e -w ad-hoc-resizing-partitioned-e2e -t $thread -k 100000000 -e 100000000 --capacity 50000000 --breakdown
done
