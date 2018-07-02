#!/usr/bin/env bash

set -e

# kill -L lists all signals in the nice table
# catch ctl-c signal and stop all background processes
trap 'jobs -p | xargs kill -TERM' SIGINT

python ppqueue.py &
for i in {1..4}
do
    python ppworker.py &
    sleep 1
done

python lpclient.py c1 &
python lpclient.py c2 &

sleep infinity
