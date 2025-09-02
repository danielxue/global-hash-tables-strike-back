#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. ${script_dir}/run_workload.sh

output_file=${script_dir}/../data/scaling_experiment.csv
echo_columns $output_file

for thread in "$@"; do
    ## Low cardinality (1000 / 100 million elements), ~100% lookup/~0% insertion.
    # Uniform data distribution.
    bench $output_file $ticketing $update $e2e -t $thread -k 1000 -e 100000000

    ## High cardinality (10 million keys / 100 million elements), 90% lookup/10% insertion.
    # Uniform data distribution.
    bench $output_file $ticketing $update $e2e -t $thread -k 10000000 -e 100000000

    # Skewed data distribution.
    bench $output_file $ticketing $update $e2e -t $thread -k 10000000 -e 100000000 --zipf=0.8

    # Heavy hitter data distribution.
    bench $output_file $ticketing $update $e2e -t $thread -k 10000000 -e 100000000 --heavy-hitter=0.5

    ## Unique keys (100 million keys / 100 million elements), 0% lookup/100% insertion.
    # Uniform data distribution.
    bench $output_file $ticketing $update $e2e -t $thread -k 100000000 -e 100000000
done
