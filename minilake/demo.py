import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


EVENT_TYPES = ("page_view", "search", "add_to_cart", "purchase")
EVENT_WEIGHTS = (0.55, 0.20, 0.15, 0.10)
COUNTRIES = ("US", "CA", "GB", "IN", "DE")


def generate_demo(path: Path, rows: int = 50_000, seed: int = 7) -> Path:
    if rows < 1:
        raise ValueError("rows must be positive")
    randomizer = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("event_id", "user_id", "event_type", "event_ts", "amount", "country"),
        )
        writer.writeheader()
        for index in range(rows):
            event_type = randomizer.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
            timestamp = start + timedelta(
                days=randomizer.randrange(30),
                seconds=randomizer.randrange(86_400),
            )
            writer.writerow(
                {
                    "event_id": f"evt-{index:09d}",
                    "user_id": f"usr-{randomizer.randrange(max(100, rows // 20)):06d}",
                    "event_type": event_type,
                    "event_ts": timestamp.isoformat().replace("+00:00", "Z"),
                    "amount": f"{randomizer.uniform(5, 500):.2f}" if event_type == "purchase" else "",
                    "country": randomizer.choice(COUNTRIES),
                }
            )
    return path

