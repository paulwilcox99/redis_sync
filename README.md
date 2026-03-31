# Redis Simulated Time Coordination

A simulated-time coordination system using Redis as the coordination backend.
Workers have dynamic task schedules and must all be present before the
simulation starts (enforced by a ready gate). Any crash requires a full reset.

## Design Philosophy

- The sorted set **IS** the schedule — controller never scans worker hashes
- The pending ACK set **IS** the sync gate — completion check is O(1) SCARD
- Redis Streams are the coordination bus — workers sleep on XREAD BLOCK
- Lua scripts handle all critical sections atomically
- Worker hashes exist for human inspection only — no coordination logic reads them
- **Fail fast** — any crash errors out loudly, full reset is the recovery procedure

## Quick Start

### 1. Environment Setup

**Linux/macOS:**
```bash
bash setup.sh
source venv/bin/activate
```

**Windows:**
```bat
setup.bat
venv\Scripts\activate
```

### 2. Initialize Redis

```bash
python setup_redis.py
```

### 3. Start the System (4 terminals)

```bash
# Terminal 1
python controller.py

# Terminal 2
python worker.py worker_a

# Terminal 3
python worker.py worker_b

# Terminal 4
python worker.py worker_c
```

Workers must all start within `READY_TIMEOUT_SECONDS` (default: 120s).

### 4. Reset After Crash

```bash
python setup_redis.py
```

Then restart controller and all workers. No partial recovery — always restart fresh.

## File Structure

```
redis_sync/
├── config.py           # All constants and Redis key names
├── setup_redis.py      # Initializes all Redis structures, clears previous state
├── controller.py       # Ready gate, clock owner, ACK gate manager
├── worker.py           # Registers, waits for start, executes tasks, ACKs
├── scripts/
│   ├── ack.lua         # Atomic ACK: remove from pending, return remaining count
│   └── advance_clock.lua  # Atomic clock advance: update time, rebuild ACK set,
│                          #   write to stream, return due worker count
├── requirements.txt
├── setup.sh
├── setup.bat
└── .gitignore
```

## Customization

The only function you need to change for real business logic is
`get_next_event_hours(worker_id, sim_time)` in `worker.py`.

```python
def get_next_event_hours(worker_id: str, sim_time: datetime) -> int:
    # Return sim hours until this worker's next task.
    # Branch on worker_id for per-worker behavior.
    # Can read from Redis, files, APIs, or databases.
    pass
```

## Redis Inspection Commands

```bash
# Simulation state
redis-cli GET sim:clock:time
redis-cli GET sim:clock:status

# Schedule: who acts next (scores are Unix timestamps)
redis-cli ZRANGE sim:schedule 0 -1 WITHSCORES

# Who still needs to ACK this cycle
redis-cli SMEMBERS sim:pending_acks

# Ready gate status
redis-cli SMEMBERS sim:workers:registered
redis-cli SMEMBERS sim:workers:ready

# Worker metadata
redis-cli HGETALL sim:worker:worker_a

# Event stream (most recent 20)
redis-cli XREVRANGE sim:events + - COUNT 20

# Watch live events
redis-cli XREAD BLOCK 0 STREAMS sim:events $

# Count events so far
redis-cli XLEN sim:events
```

## Redis Key Design

| Key | Type | Purpose |
|-----|------|---------|
| `sim:clock:time` | String | Current simulation time (ISO datetime) |
| `sim:clock:status` | String | `INIT` \| `WAITING_FOR_WORKERS` \| `RUNNING` \| `DONE` |
| `sim:schedule` | Sorted Set | Worker → next event Unix timestamp |
| `sim:pending_acks` | Set | Workers due this cycle that haven't ACK'd yet |
| `sim:workers:registered` | Set | All known worker IDs (from config) |
| `sim:workers:ready` | Set | Workers that have checked in on startup |
| `sim:events` | Stream | `SIMULATION_START` \| `CLOCK_ADVANCE` \| `SIMULATION_DONE` |
| `sim:worker:<id>` | Hash | Per-worker metadata (inspection only) |

## Configuration (`config.py`)

| Setting | Default | Description |
|---------|---------|-------------|
| `SIM_START` | 2026-01-01 UTC | Simulation start datetime |
| `SIM_END` | 2026-03-31 UTC | Simulation end datetime |
| `POLL_INTERVAL_MS` | 1000 | XREAD BLOCK timeout |
| `WORKER_TIMEOUT_SECONDS` | 30 | Stall warning threshold |
| `READY_TIMEOUT_SECONDS` | 120 | How long controller waits for all workers |
| `WORKERS` | `["worker_a", "worker_b", "worker_c"]` | Worker IDs |
