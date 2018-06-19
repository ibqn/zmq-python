#!/usr/bin/env bash

set +e

echo "Starting subscribers..."
for ((i=0; i<10; i++))
do
    exec python syncsub.py &
    #python syncsub.py &
done
echo "Starting publisher..."
exec python syncpub.py
#python syncpub.py

exit 0
