from datetime import datetime
import os
import subprocess

import config
from croniter import croniter


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _crontab_path(worker_id: str) -> str:
    specific = os.path.join(_PROJECT_ROOT, f"crontab.{worker_id}")
    if os.path.exists(specific):
        return specific
    return os.path.join(_PROJECT_ROOT, "crontab")


def _parse_crontab(path: str) -> list[tuple[str, str]]:
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 5)
            if len(parts) < 6:
                continue
            _minute, hour, day, month, weekday, command = parts
            # minute kept in parse but normalized to 0; sim clock runs at whole-hour granularity
            entries.append((f"0 {hour} {day} {month} {weekday}", command))
    return entries


def _dormant_hours(sim_time: datetime) -> int:
    return max(1, int((config.SIM_END - sim_time).total_seconds() / 3600) + 1)


def run(worker_id: str, sim_time: datetime) -> int:
    path = _crontab_path(worker_id)
    entries = _parse_crontab(path)

    if not entries:
        print(f"[{worker_id}] crontab empty, going dormant")
        return _dormant_hours(sim_time)

    # croniter requires naive UTC datetimes
    sim_naive = sim_time.replace(tzinfo=None)
    sim_end_naive = config.SIM_END.replace(tzinfo=None)

    for expr, command in entries:
        if croniter.match(expr, sim_naive):
            print(f"[{worker_id}] {sim_time.isoformat()} running: {command}")
            subprocess.Popen(command, shell=True)

    next_fire = None
    for expr, _ in entries:
        candidate = croniter(expr, sim_naive).get_next(datetime)
        if candidate <= sim_end_naive:
            if next_fire is None or candidate < next_fire:
                next_fire = candidate

    if next_fire is None:
        print(f"[{worker_id}] no future jobs within simulation, going dormant")
        return _dormant_hours(sim_time)

    return max(1, int((next_fire - sim_naive).total_seconds() / 3600))
