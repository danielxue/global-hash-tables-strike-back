#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. ${script_dir}/run_workload.sh

output_file=${script_dir}/../data/resizing_experiment.csv
echo_columns $output_file

for thread in "$@"; do
    for workload in "atomic-e2e" "thread-local-e2e" "partitioned-e2e"; do
        ## High cardinality (10 million keys / 100 million elements), 90% lookup/10% insertion.
        # No resizing.
        bench $output_file -w $workload -t $thread -k 10000000 -e 100000000

        if [ "$workload" != "partitioned-e2e" ]; then
            # Half capacity (should resize once).
            bench $output_file -w $workload -t $thread -k 10000000 -e 100000000 --capacity 5000000
        fi

        ## Unique keys (100 million keys / 100 million elements), 0% lookup/100% insertion.
        # No resizing.
        bench $output_file -w $workload -t $thread -k 100000000 -e 100000000

        if [ "$workload" != "partitioned-e2e" ]; then
            # Half capacity (should resize once).
            bench $output_file -w $workload -t $thread -k 100000000 -e 100000000 --capacity 50000000
        fi
    done
done

