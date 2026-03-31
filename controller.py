"""
controller.py — Ready gate, clock owner, ACK gate manager.

Responsibilities:
  1. Wait for all workers to register (ready gate)
  2. Seed the schedule and fire SIMULATION_START
  3. Advance the simulation clock each time all ACKs are collected
  4. Detect stalls and ERROR-state workers
  5. Exit cleanly on SIMULATION_DONE

Run after setup_redis.py, before workers.
"""

import os
import sys
import time
from datetime import datetime, timezone

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

def fatal(message: str):
    print(f"[CONTROLLER] FATAL: {message}")
    print("[CONTROLLER] Run setup_redis.py to reset and start over.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
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
        fatal(f"Cannot connect to Redis — {e}")

    # Load Lua scripts once at startup
    ACK_SCRIPT = _load_script(r, "ack.lua")           # noqa: F841 (used by workers)
    ADVANCE_CLOCK_SCRIPT = _load_script(r, "advance_clock.lua")

    sim_end_ts = str(config.SIM_END.timestamp())

    # -----------------------------------------------------------------------
    # Startup: verify clean state
    # -----------------------------------------------------------------------
    status = r.get(config.KEY_CLOCK_STATUS)
    if status != "INIT":
        fatal(
            f"sim:clock:status is {status!r}, expected 'INIT'. "
            "Is the controller already running, or was setup_redis.py skipped?"
        )

    # -----------------------------------------------------------------------
    # Ready gate
    # -----------------------------------------------------------------------
    r.set(config.KEY_CLOCK_STATUS, "WAITING_FOR_WORKERS")
    print(f"[CONTROLLER] Waiting for workers. Timeout: "
          f"{config.READY_TIMEOUT_SECONDS} seconds.")

    elapsed = 0
    while elapsed < config.READY_TIMEOUT_SECONDS:
        missing = r.sdiff(config.KEY_WORKERS_REGISTERED, config.KEY_WORKERS_READY)
        if not missing:
            break
        if elapsed % 5 == 0:
            print(f"[CONTROLLER] Waiting for: {missing}")
        time.sleep(1)
        elapsed += 1

    missing = r.sdiff(config.KEY_WORKERS_REGISTERED, config.KEY_WORKERS_READY)
    all_workers = set(config.WORKERS)
    ready = r.smembers(config.KEY_WORKERS_READY)

    if missing == all_workers or not ready:
        fatal("No workers checked in. Start workers and rerun setup_redis.py.")

    if missing:
        fatal(
            f"Workers never checked in: {missing}. "
            "All workers must be present before the simulation begins. "
            "Run setup_redis.py and restart."
        )

    # -----------------------------------------------------------------------
    # All workers present — seed schedule and fire SIMULATION_START
    # -----------------------------------------------------------------------
    r.set(config.KEY_CLOCK_STATUS, "RUNNING")

    # Controller owns the initial schedule population (not workers themselves).
    # This ensures a consistent starting state before the first XREAD message.
    for worker_id in config.WORKERS:
        r.zadd(config.KEY_SCHEDULE, {worker_id: config.SIM_START.timestamp()})

    result = ADVANCE_CLOCK_SCRIPT(
        keys=[
            config.KEY_CLOCK_TIME,
            config.KEY_CLOCK_STATUS,
            config.KEY_SCHEDULE,
            config.KEY_PENDING_ACKS,
            config.KEY_EVENTS_STREAM,
        ],
        args=[
            config.SIM_START.isoformat(),
            str(config.SIM_START.timestamp()),
            sim_end_ts,
            "SIMULATION_START",
        ],
    )

    print(f"[CONTROLLER] All workers ready. Simulation starting. "
          f"{result} workers due in first cycle.")

    # -----------------------------------------------------------------------
    # Main loop
    # -----------------------------------------------------------------------
    last_stream_id = "0-0"   # read from the beginning of existing messages
    last_advance_real_time = time.time()

    while True:
        # ---- Check SCARD on every iteration (O(1), efficient) --------------
        remaining = r.scard(config.KEY_PENDING_ACKS)
        if remaining == 0:
            result_val = r.zrange(config.KEY_SCHEDULE, 0, 0, withscores=True)
            if not result_val:
                # All workers removed themselves (final tasks past SIM_END).
                # Advance clock to SIM_END to trigger SIMULATION_DONE.
                ADVANCE_CLOCK_SCRIPT(
                    keys=[
                        config.KEY_CLOCK_TIME,
                        config.KEY_CLOCK_STATUS,
                        config.KEY_SCHEDULE,
                        config.KEY_PENDING_ACKS,
                        config.KEY_EVENTS_STREAM,
                    ],
                    args=[
                        config.SIM_END.isoformat(),
                        str(config.SIM_END.timestamp()),
                        sim_end_ts,
                        "CLOCK_ADVANCE",
                    ],
                )
                last_advance_real_time = time.time()
                # SIMULATION_DONE will arrive in stream; wait for it.
            else:
                next_worker, next_ts = result_val[0]
                next_time = datetime.fromtimestamp(next_ts, tz=timezone.utc)

                result = ADVANCE_CLOCK_SCRIPT(
                    keys=[
                        config.KEY_CLOCK_TIME,
                        config.KEY_CLOCK_STATUS,
                        config.KEY_SCHEDULE,
                        config.KEY_PENDING_ACKS,
                        config.KEY_EVENTS_STREAM,
                    ],
                    args=[
                        next_time.isoformat(),
                        str(next_ts),
                        sim_end_ts,
                        "CLOCK_ADVANCE",
                    ],
                )
                last_advance_real_time = time.time()

                if result == "DONE":
                    # Wait for SIMULATION_DONE message to arrive in stream
                    pass
                elif result == 0:
                    # No workers due at this time — skip forward
                    _skip_forward(r, ADVANCE_CLOCK_SCRIPT, sim_end_ts,
                                   last_advance_real_time)
                    last_advance_real_time = time.time()
                else:
                    print(f"[CONTROLLER] Sim time -> {next_time.isoformat()} "
                          f"({result} workers due)")

        # ---- Read stream for SIMULATION_DONE notification ------------------
        msgs = r.xread(
            {config.KEY_EVENTS_STREAM: last_stream_id},
            count=10,
            block=config.POLL_INTERVAL_MS,
        )

        if not msgs:
            # Timeout — check for stalls
            _check_stalls(r, last_advance_real_time)
            continue

        for _stream_name, entries in msgs:
            for msg_id, fields in entries:
                last_stream_id = msg_id
                msg_type = fields.get("type")

                if msg_type == "SIMULATION_DONE":
                    _print_final_summary(r)
                    return

                # SIMULATION_START / CLOCK_ADVANCE — controller sent these,
                # no action needed here beyond updating last_stream_id.


def _skip_forward(r, advance_clock_script, sim_end_ts, last_advance_real_time):
    """Advance clock until workers are due or simulation ends."""
    max_skips = 10000
    skips = 0
    while True:
        result_val = r.zrange(config.KEY_SCHEDULE, 0, 0, withscores=True)
        if not result_val:
            fatal("Schedule empty during skip-forward.")

        next_worker, next_ts = result_val[0]
        next_time = datetime.fromtimestamp(next_ts, tz=timezone.utc)

        result = advance_clock_script(
            keys=[
                config.KEY_CLOCK_TIME,
                config.KEY_CLOCK_STATUS,
                config.KEY_SCHEDULE,
                config.KEY_PENDING_ACKS,
                config.KEY_EVENTS_STREAM,
            ],
            args=[
                next_time.isoformat(),
                str(next_ts),
                sim_end_ts,
                "CLOCK_ADVANCE",
            ],
        )

        if result == "DONE":
            return
        if result > 0:
            print(f"[CONTROLLER] Sim time -> {next_time.isoformat()} "
                  f"({result} workers due) [skip-forward]")
            return

        skips += 1
        if skips > max_skips:
            fatal("Skip-forward limit exceeded (10000 iterations). "
                  "Check worker schedules for an infinite gap.")


def _check_stalls(r, last_advance_real_time: float):
    """Warn about stalled workers; fatal if any are in ERROR state."""
    elapsed = time.time() - last_advance_real_time
    if elapsed <= config.WORKER_TIMEOUT_SECONDS:
        return

    pending = r.smembers(config.KEY_PENDING_ACKS)
    for worker_id in pending:
        status = r.hget(f"{config.KEY_WORKER_META}{worker_id}", "status")
        if status == "ERROR":
            fatal(f"Worker {worker_id} is in ERROR state.")
        print(f"[CONTROLLER] WARNING: {worker_id} stalled (status={status})")


def _print_final_summary(r):
    print("\n[CONTROLLER] Simulation complete. Final worker state:")
    for worker_id in config.WORKERS:
        meta = r.hgetall(f"{config.KEY_WORKER_META}{worker_id}")
        last_ack = meta.get("last_ack_time", "N/A")
        status = meta.get("status", "N/A")
        print(f"  {worker_id}: status={status}, last_ack_time={last_ack}")
    print("[CONTROLLER] Done.")


# ---------------------------------------------------------------------------
# Validation trace — two complete cycles
# ---------------------------------------------------------------------------
#
# Step 0 — setup_redis.py:
#   GET  sim:clock:time              -> "2026-01-01T00:00:00+00:00"
#   GET  sim:clock:status            -> "INIT"
#   SMEMBERS sim:workers:registered  -> {"worker_a", "worker_b", "worker_c"}
#   ZRANGE   sim:schedule 0 -1       -> (empty)
#   XLEN     sim:events              -> 0
#
# Step 1 — Controller starts ready gate:
#   SET sim:clock:status "WAITING_FOR_WORKERS"
#   worker_a: SADD sim:workers:ready worker_a
#             HSET sim:worker:worker_a status READY last_seen <now>
#   worker_b: SADD sim:workers:ready worker_b
#   worker_c: SADD sim:workers:ready worker_c
#   SDIFF sim:workers:registered sim:workers:ready -> (empty, all present)
#   SET sim:clock:status "RUNNING"
#   ZADD sim:schedule 1735689600 worker_a   (SIM_START ts)
#   ZADD sim:schedule 1735689600 worker_b
#   ZADD sim:schedule 1735689600 worker_c
#   advance_clock.lua(SIM_START, "SIMULATION_START"):
#     SET  sim:clock:time "2026-01-01T00:00:00+00:00"
#     ZRANGEBYSCORE sim:schedule 0 1735689600 -> [worker_a, worker_b, worker_c]
#     DEL  sim:pending_acks
#     SADD sim:pending_acks worker_a worker_b worker_c
#     XADD sim:events * type SIMULATION_START sim_time "2026-01-01T00:00:00+00:00"
#     returns 3
#
# Step 2 — Workers process SIMULATION_START (sim_time = 2026-01-01T00:00:00):
#   All three workers read SIMULATION_START from stream.
#   Each checks: ZSCORE sim:schedule <id> -> 1735689600 == sim_time ts -> due.
#   Each checks: SISMEMBER sim:pending_acks <id> -> 1 -> confirmed due.
#
#   worker_a: get_next_event_hours -> 8
#     ZADD sim:schedule 1735718400 worker_a   (2026-01-01T08:00:00)
#     HSET sim:worker:worker_a status ACK last_ack_time "2026-01-01T00:00:00+00:00"
#     ack.lua: SREM sim:pending_acks worker_a -> SCARD -> 2
#
#   worker_b: get_next_event_hours -> 24
#     ZADD sim:schedule 1735776000 worker_b   (2026-01-02T00:00:00)
#     ack.lua: SREM sim:pending_acks worker_b -> SCARD -> 1
#
#   worker_c: get_next_event_hours -> 48
#     ZADD sim:schedule 1735862400 worker_c   (2026-01-03T00:00:00)
#     ack.lua: SREM sim:pending_acks worker_c -> SCARD -> 0
#
# Step 3 — Controller detects SCARD == 0, advances clock:
#   ZRANGE sim:schedule 0 0 WITHSCORES -> [(worker_a, 1735718400)]
#   next_time = 2026-01-01T08:00:00
#   advance_clock.lua(2026-01-01T08:00:00, "CLOCK_ADVANCE"):
#     SET  sim:clock:time "2026-01-01T08:00:00+00:00"
#     ZRANGEBYSCORE sim:schedule 0 1735718400 -> [worker_a]
#     DEL  sim:pending_acks
#     SADD sim:pending_acks worker_a
#     XADD sim:events * type CLOCK_ADVANCE sim_time "2026-01-01T08:00:00+00:00"
#     returns 1
#   Print: "[CONTROLLER] Sim time -> 2026-01-01T08:00:00+00:00 (1 workers due)"
#
# Step 4 — Workers read CLOCK_ADVANCE (sim_time = 2026-01-01T08:00:00):
#   worker_a: ZSCORE sim:schedule worker_a -> 1735718400 == sim_time ts -> due
#             SISMEMBER sim:pending_acks worker_a -> 1 -> confirmed
#             get_next_event_hours -> 12
#             ZADD sim:schedule 1735761600 worker_a   (2026-01-01T20:00:00)
#             ack.lua: SREM -> SCARD -> 0
#   worker_b: ZSCORE -> 1735776000 > 1735718400 -> not due, skip
#   worker_c: ZSCORE -> 1735862400 > 1735718400 -> not due, skip
#
#   Redis state after cycle 2:
#     sim:clock:time     = "2026-01-01T08:00:00+00:00"
#     sim:clock:status   = "RUNNING"
#     sim:pending_acks   = {} (empty)
#     sim:schedule       = {worker_a: 1735761600,   <- 2026-01-01T20:00:00
#                           worker_b: 1735776000,   <- 2026-01-02T00:00:00
#                           worker_c: 1735862400}   <- 2026-01-03T00:00:00


if __name__ == "__main__":
    main()
