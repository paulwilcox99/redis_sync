#!/usr/bin/env bash
set -e

PYTHON=venv/bin/python3

$PYTHON setup_redis.py

echo ""
echo "Starting controller and workers..."
echo "Logs: /tmp/sim_controller.log, /tmp/sim_worker_<id>.log"
echo "Press Ctrl+C to stop."
echo ""

$PYTHON controller.py > /tmp/sim_controller.log 2>&1 &
PIDS=$!

for worker in $(python3 -c "import config; print(' '.join(config.WORKERS))"); do
    $PYTHON worker.py "$worker" > "/tmp/sim_worker_${worker}.log" 2>&1 &
    PIDS="$PIDS $!"
done

# Print logs from all processes to stdout as they arrive
tail -f /tmp/sim_controller.log $(for w in $(venv/bin/python3 -c "import config; print(' '.join(config.WORKERS))"); do echo "/tmp/sim_worker_${w}.log"; done) &
TAIL_PID=$!

# Wait for all sim processes; kill tail when they finish
wait $PIDS
kill $TAIL_PID 2>/dev/null

echo ""
echo "Simulation complete."
