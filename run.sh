#!/usr/bin/env bash
set -e

PYTHON=venv/bin/python3
PIDS=""

cleanup() {
    echo ""
    echo "Stopping simulation..."
    kill $PIDS 2>/dev/null
    wait $PIDS 2>/dev/null
    exit 0
}
trap cleanup INT TERM

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
