# Developer Guide

This guide explains how to create new worker tasks and assemble them into a simulation model.

## Concepts

A **task** is a Python function that executes one unit of work for a worker at a given simulated time and returns how many simulated hours until that worker should fire again.

A **model** is the set of workers defined in `config.py` and the task function assigned to each one.

The coordination framework (`controller.py`, `worker.py`, Lua scripts) never changes. All customization lives in two places:
- `tasks/` — one file per task type, plus an optional model init file
- `config.py` — worker list, init function, and task assignments

---

## Task Function Contract

Every task function must have this exact signature:

```python
def run(worker_id: str, sim_time: datetime) -> int:
    ...
    return hours
```

| Parameter | Type | Description |
|---|---|---|
| `worker_id` | `str` | ID of the worker executing this task (e.g. `"worker_a"`) |
| `sim_time` | `datetime` | Current simulated time (UTC, timezone-aware) |
| return value | `int` | Simulated hours until this worker's next task (must be > 0) |

The function is called exactly when the simulated clock reaches the worker's scheduled time. It executes synchronously — the clock does not advance until all due workers have returned from their task functions and ACK'd.

**What to do inside the function:**
- Perform the real computation, DB writes, API calls, etc.
- Read and write any external state you need
- Print progress if useful
- Return the number of sim-hours until this worker should be called again

**What not to do:**
- Do not return 0 or a negative number
- Do not write to any `sim:` key — those are owned by the framework (reading them is fine)
- Do not call `sys.exit()` — raise an exception instead; the framework will catch it and mark the worker as ERROR

---

## Creating a New Task

Create a file in `tasks/`. The filename becomes the task name. Export a single `run` function.

```
tasks/my_task.py
```

```python
from datetime import datetime


def run(worker_id: str, sim_time: datetime) -> int:
    # do work here
    print(f"[{worker_id}] doing my_task at {sim_time.isoformat()}")
    return 24  # next event in 24 sim-hours
```

If your task needs Redis access, accept a redis client as a module-level dependency or create it inside the function using the same connection settings as `config.py`.

---

## Two-Phase Tasks (Start and End Actions)

If you need to take an action at the start of a task and a different action when it completes, store in-progress state in Redis between the two firings.

```python
import redis
import config
from datetime import datetime

STATE_KEY = "sim:task_state:{worker_id}"

def run(worker_id: str, sim_time: datetime) -> int:
    r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT,
                    db=config.REDIS_DB, decode_responses=True)
    key = STATE_KEY.format(worker_id=worker_id)
    state = r.hgetall(key)

    if not state:
        # --- TASK START ---
        value = r.get("sim:state:some_counter") or "0"
        r.hset(key, mapping={"duration_hours": "4", "value_at_start": value})
        print(f"[{worker_id}] Task started at {sim_time.isoformat()}, read value={value}")
        return 4  # wake me again in 4 sim-hours to finish

    else:
        # --- TASK END ---
        new_value = int(state["value_at_start"]) + 1
        r.set("sim:state:some_counter", new_value)
        r.delete(key)
        print(f"[{worker_id}] Task finished at {sim_time.isoformat()}, wrote value={new_value}")
        return 1  # pick up next task next hour
```

The worker fires at hour N (start), stores state, returns 4. It fires again at hour N+4 (end), reads state, writes result, cleans up.

---

## Model Initialization

If your model needs to set up shared resources before the simulation starts (databases, files, shared state), define an init function in `tasks/` and assign it to `SIM_INIT` in `config.py`. It is called once by `setup_redis.py` before any workers or the controller start.

### Init function contract

```python
def run(workers: list):
    # workers is the list of worker IDs from config.WORKERS
    # set up anything the model needs here
```

The `workers` argument lets the init function set up only what the active workers need — useful when different models use different subsets of resources.

### Example

```python
# tasks/init_my_model.py
import sqlite3

def run(workers: list):
    conn = sqlite3.connect("sim_data.db")
    conn.execute("CREATE TABLE IF NOT EXISTS events (worker TEXT, sim_time TEXT, value INTEGER)")
    conn.commit()
    conn.close()
    print(f"[INIT] Database ready for workers: {workers}")
```

```python
# config.py
from tasks.init_my_model import run as sim_init
SIM_INIT = sim_init
```

`SIM_INIT` is optional — if not defined in `config.py`, `setup_redis.py` skips the init step.

---

## Configuring a Model

A model is defined entirely in `config.py`. To create a new model:

### 1. Set the worker list

```python
WORKERS = ["worker_a", "worker_b", "worker_c"]
```

Worker IDs are arbitrary strings. Add or remove workers here. Every ID in this list must have an entry in `WORKER_TASKS`.

### 2. Set the simulation time range

```python
SIM_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
SIM_END   = datetime(2026, 3, 31, tzinfo=timezone.utc)
```

### 3. Import and assign the init function

```python
from tasks.init_my_model import run as sim_init
SIM_INIT = sim_init
```

This is optional. If omitted, no init step runs.

### 4. Import task functions and assign them

```python
from tasks.my_task import run as my_task
from tasks.other_task import run as other_task
from tasks.random_test import run as random_test

WORKER_TASKS = {
    "worker_a": my_task,
    "worker_b": other_task,
    "worker_c": my_task,   # two workers can share the same task function
}
```

Every worker ID in `WORKERS` must appear in `WORKER_TASKS`. Multiple workers can share the same task function — `worker_id` is passed in so the function can branch on it if needed.

### 5. Run setup and start

```bash
bash run.sh
```

Or manually:

```bash
venv/bin/python3 setup_redis.py      # runs SIM_INIT then initializes Redis
venv/bin/python3 controller.py       # terminal 1
venv/bin/python3 worker.py worker_a  # terminal 2
venv/bin/python3 worker.py worker_b  # terminal 3
venv/bin/python3 worker.py worker_c  # terminal 4
```

---

## Managing Multiple Models

If you need to switch between models without editing `config.py` each time, create separate config files and symlink or copy as needed:

```
config_model_a.py
config_model_b.py
config.py  ← copy/symlink whichever model you want to run
```

Or import the desired model config at the top of `config.py`:

```python
from model_configs.model_a import WORKERS, WORKER_TASKS, SIM_START, SIM_END
```

---

## Reference: Shared Redis State

Task functions may read any `sim:` key for observability or decision-making. Writing is restricted to keys the framework does not own — all `sim:` coordination keys are read-only from a task's perspective. Use any non-`sim:` key namespace freely for your own state.

| Key | Type | Contains | Safe to read | Safe to write |
|---|---|---|---|---|
| `sim:clock:time` | String | Current simulated time as ISO datetime string | yes | no |
| `sim:clock:status` | String | `INIT` / `WAITING_FOR_WORKERS` / `RUNNING` / `DONE` | yes | no |
| `sim:schedule` | Sorted Set | Worker ID → Unix timestamp of next scheduled event, sorted ascending | yes | no |
| `sim:pending_acks` | Set | Worker IDs that are due this tick but have not yet ACK'd | yes | no |
| `sim:events` | Stream | `SIMULATION_START`, `CLOCK_ADVANCE`, `SIMULATION_DONE` messages, each with a `sim_time` field | yes | no |
| `sim:workers:registered` | Set | Worker IDs expected by the simulation (seeded from `config.WORKERS`) | yes | no |
| `sim:workers:ready` | Set | Worker IDs that have checked in at startup | yes | no |
| `sim:worker:<id>` | Hash | Per-worker metadata: `status`, `last_seen`, `last_ack_time` | yes | no |
| Any other key | — | Your own simulation state | yes | yes |

---

## Cron Worker

The bundled `tasks/cron.py` provides a cron-style scheduler that fires jobs based on simulated time rather than wall-clock time. It reads a standard crontab file and executes matching entries as fire-and-forget subprocesses each time the worker is called.

### Crontab file

Place a file named `crontab` in the project root. Standard 5-field syntax:

```
# Fields: minute  hour  day-of-month  month  day-of-week  command
0 9 * * *    venv/bin/python3 my_script.py arg1
0 */12 * * * venv/bin/python3 another_script.py
```

Supported field syntax: `*`, `n`, `*/n`, `n-m`, `n,m,o`, `n-m/s`.

**Minute field:** parsed but ignored — the sim clock advances in whole hours, so all jobs fire on hour boundaries regardless of the minute value. Keep this field as a placeholder for future sub-hour support.

For multiple cron workers, name the file `crontab.<worker_id>` (e.g. `crontab.cron_a`). The worker looks for its specific file first and falls back to `crontab`.

### How it schedules

On each firing the worker:
1. Checks every crontab entry against the current `sim_time`
2. Launches all matching commands via `subprocess.Popen` (fire-and-forget — jobs run independently and do not block the sim tick)
3. Finds the earliest next firing time across all entries and returns that many sim-hours

When no future jobs remain within `SIM_END`, the worker schedules itself past the end of the simulation and goes dormant.

### Wiring it up in `config.py`

```python
from tasks.cron import run as cron_task

WORKERS = [..., "cron"]

WORKER_TASKS = {
    ...,
    "cron": cron_task,
}
```

---

## Examples

### `tasks/init_model.py`

The bundled init example prints the worker list and exits. Use it to verify init is wired up correctly before adding real setup logic.

```python
def run(workers: list):
    print(f"[INIT] Initializing model with workers: {workers}")
```

### `tasks/cron.py`

The bundled cron worker reads `crontab` from the project root, executes matching entries as subprocesses at the correct simulated time, and returns the hours to the next scheduled job. See the [Cron Worker](#cron-worker) section above for full details.

### `tasks/random_test.py`

The bundled task picks a random interval, sleeps briefly to simulate real work, and returns. Use it to verify the full simulation loop before adding real task logic.

```python
import random
import time
from datetime import datetime


def run(worker_id: str, sim_time: datetime) -> int:
    hours = random.choice([1, 2, 4, 8, 12, 24, 48])
    time.sleep(random.uniform(1, 3))
    print(f"[{worker_id}] Task at {sim_time.isoformat()}: next in {hours} sim hours")
    return hours
```
