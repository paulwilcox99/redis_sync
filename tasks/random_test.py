import random
import time
from datetime import datetime


def run(worker_id: str, sim_time: datetime) -> int:
    hours = random.choice([1, 2, 4, 8, 12, 24, 48])
    time.sleep(random.uniform(1, 3))
    print(f"[{worker_id}] Task at {sim_time.isoformat()}: next in {hours} sim hours")
    return hours
