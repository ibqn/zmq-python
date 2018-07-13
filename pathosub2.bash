#!/usr/bin/env bash


# stop on first error
set -e

# kill -L lists all signals in the nice table
# catch ctl-c signal and stop all background processes
trap 'jobs -p | xargs kill -TERM' SIGINT

python lvcache.py &
python pathopub.py 'tcp://localhost:5557' &
python pathosub.py 'tcp://localhost:5558' &

sleep infinity
