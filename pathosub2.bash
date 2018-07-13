#!/usr/bin/env bash


# stop on first error
set -e

# kill -L lists all signals in the nice table
# catch ctl-c signal and stop all background processes
trap 'jobs -p | xargs kill -TERM' SIGINT

python lvcache.py &
python pathopub.py 'tcp://*:5557' &

echo 'start first subscription'
python pathosub.py 'tcp://localhost:5558' &

echo 'start second subscription 5s later'
sleep 5s
python pathosub.py 'tcp://localhost:5558' &

sleep infinity
