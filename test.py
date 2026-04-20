import sys
from datetime import datetime, timezone

now = datetime.now(timezone.utc).isoformat()
print(f"[test.py] executed at real time {now}, args: {sys.argv[1:]}", flush=True)
