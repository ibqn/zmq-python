#!/bin/env/bash


# stop on first error
set -e

# kill -L lists all signals in the nice table
# catch ctl-c signal and stop all background processes
trap 'jobs -p | xargs kill -TERM' SIGINT

# start DC1 and connect to DC2 and DC3
python peering1.py DC1 DC2 DC3 &
# start DC2 and connect to DC1 and DC3
python peering1.py DC2 DC1 DC3 &
# start DC3 and connect to DC1 and DC2
python peering1.py DC3 DC1 DC2 &

JOBS="$(jobs -p)"
echo "Running three background jobs: "${JOBS}
sleep infinity
