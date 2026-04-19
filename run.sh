#!/usr/bin/env bash
set -e

PYTHON=venv/bin/python3

$PYTHON setup_redis.py

echo ""
echo "Starting controller and workers... Press Ctrl+C to stop."
echo ""

$PYTHON controller.py &
PIDS=$!

for worker in $($PYTHON -c "import config; print(' '.join(config.WORKERS))"); do
    $PYTHON worker.py "$worker" &
    PIDS="$PIDS $!"
done

wait $PIDS

echo ""
echo "Simulation complete."
