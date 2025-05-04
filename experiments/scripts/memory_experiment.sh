#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. ${script_dir}/run_workload.sh

output_file=${script_dir}/../data/memory_experiment.csv
echo "workload,threads,keys,elements,capacity,zipf,heavy hitter,memory,t1,t2,t3,t4,t5,t6,t7,t8,t9,initialization,ticketing,update,materialization" > $output_file

for thread in "${threads[@]}"; do
    ## Low cardinality (1000 / 100 million elements), ~100% lookup/~0% insertion.
    bench $output_file $e2e -t $thread -k 1000 -e 100000000 --breakdown

    ## High cardinality (10 million keys / 100 million elements), ~90% lookup/~10% insertion.
    bench $output_file $e2e  -t $thread -k 10000000 -e 100000000 --breakdown

    ## Unique keys (100 million keys / 100 million elements), ~0% lookup/~100% insertion.
    bench $output_file $e2e -t $thread -k 100000000 -e 100000000 --breakdown
done
