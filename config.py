from datetime import datetime, timezone

from tasks.random_test import run as random_test
from tasks.init_model import run as sim_init

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB   = 0
REDIS_PASSWORD = None

SIM_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
SIM_END   = datetime(2026, 3, 31, tzinfo=timezone.utc)

POLL_INTERVAL_MS       = 1000   # XREAD BLOCK timeout in milliseconds
WORKER_TIMEOUT_SECONDS = 30     # stall warning threshold
READY_TIMEOUT_SECONDS  = 120    # how long controller waits for all workers

WORKERS = ["worker_a", "worker_b", "worker_c"]

# Redis key constants — never hardcode keys outside this file
KEY_CLOCK_TIME         = "sim:clock:time"
KEY_CLOCK_STATUS       = "sim:clock:status"
KEY_SCHEDULE           = "sim:schedule"
KEY_PENDING_ACKS       = "sim:pending_acks"
KEY_WORKERS_REGISTERED = "sim:workers:registered"
KEY_WORKERS_READY      = "sim:workers:ready"
KEY_EVENTS_STREAM      = "sim:events"
KEY_WORKER_META        = "sim:worker:"   # append worker_id

LUA_SCRIPT_DIR = "scripts/"

SIM_INIT = sim_init

WORKER_TASKS = {
    "worker_a": random_test,
    "worker_b": random_test,
    "worker_c": random_test,
}
