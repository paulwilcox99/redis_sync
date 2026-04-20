"""
setup_redis.py — Initializes all Redis structures, clears previous state.

Run this before starting controller.py and workers.
"""

import sys
import redis
import config


def main():
    # 1. Connect and verify
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
        print(f"[SETUP] ERROR: Cannot connect to Redis at "
              f"{config.REDIS_HOST}:{config.REDIS_PORT} — {e}")
        sys.exit(1)

    print(f"[SETUP] Connected to Redis at {config.REDIS_HOST}:{config.REDIS_PORT}")

    # 2. Delete all sim:* keys using SCAN (never KEYS *)
    deleted = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="sim:*", count=100)
        if keys:
            r.delete(*keys)
            deleted += len(keys)
        if cursor == 0:
            break
    print(f"[SETUP] Cleared {deleted} existing sim:* keys.")

    # 3. Initialize base structures
    r.set(config.KEY_CLOCK_TIME, config.SIM_START.isoformat())
    r.set(config.KEY_CLOCK_STATUS, "INIT")
    r.sadd(config.KEY_WORKERS_REGISTERED, *config.WORKERS)

    # 4. sim:schedule is intentionally empty — controller populates it
    #    after the ready gate confirms all workers are present.

    # 5. Print confirmation of all sim:* keys
    print("\n[SETUP] Current sim:* state:")
    cursor = 0
    all_keys = []
    while True:
        cursor, keys = r.scan(cursor=cursor, match="sim:*", count=100)
        all_keys.extend(keys)
        if cursor == 0:
            break
    all_keys.sort()

    for key in all_keys:
        key_type = r.type(key)
        if key_type == "string":
            print(f"  {key} = {r.get(key)!r}")
        elif key_type == "set":
            print(f"  {key} = {r.smembers(key)}")
        elif key_type == "zset":
            print(f"  {key} = {r.zrange(key, 0, -1, withscores=True)}")
        elif key_type == "stream":
            print(f"  {key} (stream, len={r.xlen(key)})")
        else:
            print(f"  {key} ({key_type})")

    # 6. Run model init if defined
    if hasattr(config, 'SIM_INIT') and config.SIM_INIT:
        config.SIM_INIT(config.WORKERS)

    # 7. Instructions
    print(f"""
[SETUP] Redis initialized. Start controller.py, then start all workers
        within {config.READY_TIMEOUT_SECONDS} seconds.

  Terminal 1:  python controller.py
  Terminal 2:  python worker.py worker_a
  Terminal 3:  python worker.py worker_b
  Terminal 4:  python worker.py worker_c
""")


if __name__ == "__main__":
    main()
