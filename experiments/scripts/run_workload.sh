#!/usr/bin/env bash

# Capture interrupt.
trap ctrl_c INT
function ctrl_c() {
    exit
}

# Setup perf controls.
ctl_dir=/tmp/
ctl_fifo=${ctl_dir}perf_ctl.fifo
test -p ${ctl_fifo} && unlink ${ctl_fifo}
mkfifo ${ctl_fifo}
exec {ctl_fd}<>${ctl_fifo}

# Shared variables
ticketing_workloads=(cuckoo-map dash-map folklore-map iceberg-map leap-map)
update_workloads=(atomic-pau locking-pau thread-local-pau)
e2e_workloads=(atomic-e2e thread-local-e2e partitioned-e2e)

ticketing=""
for workload in "${ticketing_workloads[@]}"; do ticketing="${ticketing} -w ${workload}"; done
update=""
for workload in "${update_workloads[@]}"; do update="${update} -w ${workload}"; done
e2e=""
for workload in "${e2e_workloads[@]}"; do e2e="${e2e} -w ${workload}"; done

# Helpers
echo_columns() {
    echo "workload,threads,keys,elements,capacity,zeroed,value type,operator,zipf,heavy hitter,memory,t1,t2,t3,t4,t5,t6,t7,t8,t9,initialization,stage 1,stage 2,materialization" > $1
}

# Specific runners
bench() {
    b_output="$1"; shift

    RUSTFLAGS="-C target-cpu=native" cargo run --release -- \
        bench --iterations=9 --table "$@" \
    2>> bench.log >> $b_output
}

stat() {
    s_output="$1"; shift

    s_metrics=""
    if [[ -n $(perf list --no-desc 2>/dev/null | grep 'TopdownL1') ]]; then
        s_metrics="-M TopdownL1,TopdownL2"
    elif [[ -n $(perf list --no-desc 2>/dev/null | grep 'PipelineL1') ]]; then
        s_metrics="-M PipelineL1,PipelineL2"
    fi

    echo "$@" >> $s_output
    echo "disable" > $ctl_fifo
    s_stdout=$(
        RUSTFLAGS="-C target-cpu=native" \
            perf stat -e "branches,branch-misses,bus-cycles,cache-misses,cache-references,cycles,instructions,ref-cycles,stalled-cycles-frontend,stalled-cycles-backend,alignment-faults,cgroup-switches,context-switches,migrations,emulation-faults,major-faults,minor-faults,page-faults,task-clock,dTLB-loads,dTLB-load-misses,iTLB-loads,iTLB-load-misses" \
            ${s_metrics} \
            -o $s_output --append -x "," --control fd:$ctl_fd \
            -- cargo run --release -- \
                 profile --iterations=9 --control="$ctl_fifo" "$@" \
                 2>> stat.log
    )
    echo "$s_stdout" >> $s_output
    echo "" >> $s_output
}

profile() {
    p_output="$1"; shift

    CARGO_PROFILE_RELEASE_DEBUG=true RUSTFLAGS="-C target-cpu=native" \
        cargo flamegraph -o $p_output --release -- \
             bench --iterations=9 "$@"
}
