"""
worker.py — Registers, waits for start, executes tasks, ACKs.

Usage:
  python worker.py worker_a

Workers connect with "$" as stream start ID — safe because the ready gate
guarantees all workers are online before any messages are sent.
"""

import os
import random
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

import redis

import config


# ---------------------------------------------------------------------------
# Lua script loading
# ---------------------------------------------------------------------------

def _load_script(r: redis.Redis, filename: str):
    path = os.path.join(os.path.dirname(__file__), config.LUA_SCRIPT_DIR, filename)
    with open(path) as fh:
        return r.register_script(fh.read())


# ---------------------------------------------------------------------------
# Fail fast
# ---------------------------------------------------------------------------

def fatal(worker_id: str, message: str):
    print(f"[{worker_id}] FATAL: {message}")
    print(f"[{worker_id}] Run setup_redis.py to reset and start over.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Task logic — PRIMARY CUSTOMIZATION POINT
# ---------------------------------------------------------------------------

def get_next_event_hours(worker_id: str, sim_time: datetime) -> int:
    """
    Return a positive integer: sim hours until this worker's next task.

    This is the ONLY function that changes for real business logic.
    No catchup guard needed — workers only ever run live tasks.
    Branch on worker_id for completely different behavior per worker.
    Can read from Redis, files, APIs, or databases.
    """
    hours = random.choice([1, 2, 4, 8, 12, 24, 48])
    time.sleep(random.uniform(1, 3))   # simulate variable real work duration
    print(f"[{worker_id}] Task at {sim_time.isoformat()}: next in {hours} sim hours")
    return hours


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)

    worker_id = sys.argv[1]

    # 1. Validate worker_id
    if worker_id not in config.WORKERS:
        print(f"[{worker_id}] ERROR: Unknown worker_id {worker_id!r}. "
              f"Valid workers: {config.WORKERS}")
        sys.exit(1)

    # 2. Connect to Redis
    r = redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        password=config.REDIS_PASSWORD,
        decode_responses=True,
    )
    try:
        r.ping()
    except redis.exceptions.ConnectionError as e:
        fatal(worker_id, f"Cannot connect to Redis — {e}")

    # 3. Load Lua scripts
    ACK_SCRIPT = _load_script(r, "ack.lua")

    # 4. Verify simulation has not already started or finished
    status = r.get(config.KEY_CLOCK_STATUS)
    if status in ("RUNNING", "DONE"):
        fatal(
            worker_id,
            f"Simulation already started (status={status!r}). "
            "All workers must be present before simulation begins. "
            "Run setup_redis.py and restart.",
        )

    # 5. Register
    now_utc = datetime.now(tz=timezone.utc).isoformat()
    r.sadd(config.KEY_WORKERS_READY, worker_id)
    r.hset(f"{config.KEY_WORKER_META}{worker_id}",
           mapping={"status": "READY", "last_seen": now_utc})

    print(f"[{worker_id}] Registered. Waiting for all workers and "
          "simulation start...")

    # 6. Subscribe to stream from now forward ("$" = only new messages)
    last_stream_id = "$"

    # 7. Wait until controller sets status to RUNNING
    while True:
        status = r.get(config.KEY_CLOCK_STATUS)
        if status == "RUNNING":
            break
        if status == "DONE":
            fatal(worker_id, "Simulation ended before this worker started.")
        time.sleep(1)

    # Drain any messages that may have arrived during the transition to RUNNING.
    # Switch to "0-0" only if we missed the SIMULATION_START; safe to use "$"
    # because the ready gate guarantees no messages were sent before RUNNING.
    # However, we set last_stream_id to "0-0" here to catch the SIMULATION_START
    # message that was just published.
    last_stream_id = "0-0"

    print(f"[{worker_id}] Simulation started. Entering task loop.")

    # -----------------------------------------------------------------------
    # Main task loop
    # -----------------------------------------------------------------------
    try:
        _task_loop(r, worker_id, ACK_SCRIPT, last_stream_id)
    except SystemExit:
        raise
    except Exception:
        r.hset(f"{config.KEY_WORKER_META}{worker_id}",
               mapping={"status": "ERROR",
                        "last_seen": datetime.now(tz=timezone.utc).isoformat()})
        traceback.print_exc()
        print(f"[{worker_id}] Worker failed. Run setup_redis.py to reset.")
        sys.exit(1)


def _task_loop(r, worker_id: str, ACK_SCRIPT, last_stream_id: str):
    last_ack_time = None

    while True:
        msgs = r.xread(
            {config.KEY_EVENTS_STREAM: last_stream_id},
            count=10,
            block=config.POLL_INTERVAL_MS,
        )

        if not msgs:
            # Timeout — just update heartbeat and continue
            r.hset(f"{config.KEY_WORKER_META}{worker_id}",
                   "last_seen", datetime.now(tz=timezone.utc).isoformat())
            continue

        for _stream_name, entries in msgs:
            for msg_id, fields in entries:
                last_stream_id = msg_id
                msg_type = fields.get("type")

                if msg_type == "SIMULATION_DONE":
                    r.hset(f"{config.KEY_WORKER_META}{worker_id}",
                           "status", "DONE")
                    print(f"[{worker_id}] Simulation complete. "
                          f"Last task: {last_ack_time}")
                    return

                if msg_type in ("SIMULATION_START", "CLOCK_ADVANCE"):
                    _handle_clock_event(
                        r, worker_id, ACK_SCRIPT, fields, last_ack_time
                    )
                    # Update last_ack_time reference after handling
                    last_ack_time = r.hget(
                        f"{config.KEY_WORKER_META}{worker_id}", "last_ack_time"
                    ) or last_ack_time


def _handle_clock_event(r, worker_id: str, ACK_SCRIPT, fields: dict,
                        last_ack_time):
    sim_time_str = fields.get("sim_time")
    sim_time = datetime.fromisoformat(sim_time_str)
    if sim_time.tzinfo is None:
        sim_time = sim_time.replace(tzinfo=timezone.utc)

    # Check if this worker is due
    my_next_ts = r.zscore(config.KEY_SCHEDULE, worker_id)
    if my_next_ts is None:
        # Worker removed itself from schedule (past final event) — just wait
        return

    if sim_time.timestamp() < my_next_ts:
        # Not due yet
        r.hset(f"{config.KEY_WORKER_META}{worker_id}",
               "last_seen", datetime.now(tz=timezone.utc).isoformat())
        return

    # Due — verify we are in pending_acks
    if not r.sismember(config.KEY_PENDING_ACKS, worker_id):
        fatal(worker_id,
              f"Due at {sim_time_str} but not in pending_acks — state mismatch.")

    _execute_task(r, worker_id, ACK_SCRIPT, sim_time)


def _execute_task(r, worker_id: str, ACK_SCRIPT, sim_time: datetime):
    now_utc = datetime.now(tz=timezone.utc).isoformat()
    r.hset(f"{config.KEY_WORKER_META}{worker_id}",
           mapping={"status": "WORKING", "last_seen": now_utc})

    hours = get_next_event_hours(worker_id, sim_time)
    new_next_time = sim_time + timedelta(hours=hours)

    if new_next_time > config.SIM_END:
        # Final task — remove from schedule, then ACK
        r.zrem(config.KEY_SCHEDULE, worker_id)
        r.hset(f"{config.KEY_WORKER_META}{worker_id}",
               mapping={
                   "status": "DONE",
                   "last_ack_time": sim_time.isoformat(),
                   "last_seen": datetime.now(tz=timezone.utc).isoformat(),
               })
        ACK_SCRIPT(
            keys=[config.KEY_PENDING_ACKS, config.KEY_EVENTS_STREAM],
            args=[worker_id, sim_time.isoformat()],
        )
        print(f"[{worker_id}] Final task at {sim_time.isoformat()}. "
              "No more events before SIM_END.")
        # Keep reading stream until SIMULATION_DONE
        return

    # Normal task — update schedule and ACK
    new_next_ts = new_next_time.timestamp()
    r.zadd(config.KEY_SCHEDULE, {worker_id: new_next_ts})
    r.hset(f"{config.KEY_WORKER_META}{worker_id}",
           mapping={
               "status": "ACK",
               "last_ack_time": sim_time.isoformat(),
               "last_seen": datetime.now(tz=timezone.utc).isoformat(),
           })
    remaining = ACK_SCRIPT(
        keys=[config.KEY_PENDING_ACKS, config.KEY_EVENTS_STREAM],
        args=[worker_id, sim_time.isoformat()],
    )
    print(f"[{worker_id}] ACK at {sim_time.isoformat()}, "
          f"next at {new_next_time.isoformat()} "
          f"({hours} sim hours, {remaining} others pending)")


if __name__ == "__main__":
    main()
