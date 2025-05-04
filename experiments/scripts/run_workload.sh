#!/usr/bin/env bash

# capture interrupt 
trap ctrl_c INT
function ctrl_c() {
    exit
}

# setup perf controls.
ctl_dir=/tmp/
ctl_fifo=${ctl_dir}perf_ctl.fifo
test -p ${ctl_fifo} && unlink ${ctl_fifo}
mkfifo ${ctl_fifo}
exec {ctl_fd}<>${ctl_fifo}

# shared variables
threads=(1)
while test "${threads[-1]}" -lt $1; do
    threads+=($((${threads[-1]}*2)))
done

ticketing_workloads=(cuckoo-map dash-map folklore-map iceberg-map leap-map global-locking-map once-lock-map)
update_workloads=(atomic-pau global-locking-pau locking-pau thread-local-pau)
e2e_workloads=(atomic-e2e global-locking-e2e locking-e2e thread-local-e2e partitioned-e2e)

ticketing=""
for workload in "${ticketing_workloads[@]}"; do ticketing="${ticketing} -w ${workload}"; done
update=""
for workload in "${update_workloads[@]}"; do update="${update} -w ${workload}"; done
e2e=""
for workload in "${e2e_workloads[@]}"; do e2e="${e2e} -w ${workload}"; done

# specific runners
stat() {
    s_output="$1"; shift

    echo "disable" > $ctl_fifo
    RUSTFLAGS="-C target-cpu=native" \
        perf stat -e "branch-misses,branch-instructions,cache-misses,cache-references,instructions,cpu-cycles,context-switches" \
        -o $s_output --append -x "," --control fd:$ctl_fd \
        -- cargo run --release -- \
             profile --iterations=9 --control="$ctl_fifo" "$@"
}

profile() {
    p_output="$1"; shift

    CARGO_PROFILE_RELEASE_DEBUG=true RUSTFLAGS="-C target-cpu=native" \
        cargo flamegraph -o $p_output --release -- \
             bench --iterations=9 "$@"
}

bench() {
    b_output="$1"; shift

    RUSTFLAGS="-C target-cpu=native" cargo run --release -- \
        bench --iterations=9 --table "$@" \
    2>> bench.log >> $b_output
}
