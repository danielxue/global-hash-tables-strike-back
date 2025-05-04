#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. ${script_dir}/run_workload.sh

output_file=${script_dir}/../data/fuzzy_ticketing_experiment.csv
echo "workload,threads,keys,elements,capacity,zipf,heavy hitter,memory,t1,t2,t3,t4,t5,t6,t7,t8,t9" > $output_file

bench $output_file -w folklore-map  -t 32 -k 1000 -e 100000000

bench $output_file -w folklore-map  -t 32 -k 10000000 -e 100000000

bench $output_file -w folklore-map  -t 32 -k 100000000 -e 100000000

bench $output_file -w folklore-unfuzzy-map -t 32 -k 1000 -e 100000000

bench $output_file -w folklore-unfuzzy-map -t 32 -k 10000000 -e 100000000

bench $output_file -w folklore-unfuzzy-map -t 32 -k 100000000 -e 100000000
