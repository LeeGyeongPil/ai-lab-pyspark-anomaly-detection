"""Step 1: make practice sensor data and plant known anomalies.

The data is synthetic (made by code, not real machines). Because we plant the
anomalies ourselves, we know the right answer and can measure every detector.
`is_injected` and `anomaly_type` are only used to *grade* detectors, never to
compute scores.
"""

from __future__ import annotations

import math
import random

from pyspark.sql import DataFrame, SparkSession

# The four kinds of planted anomalies.
POINT_STRONG = "point_strong"  # one hour, far from normal
POINT_WEAK = "point_weak"  # one hour, only slightly outside normal noise
SUSTAINED = "sustained"  # 4-8 hours of slow upward/downward drift
COMBO = "combo"  # temperature and vibration are each plausible, the pair is not
TYPES = (POINT_STRONG, POINT_WEAK, SUSTAINED, COMBO)

SCHEMA = (
    "device string, t int, hour int, dow int, temp double, vib double, "
    "is_injected int, anomaly_type string, event_id int"
)

NOISE = 1.0  # temperature noise (std dev, degrees)
CYCLE = 8.0  # daily swing: +-8 degrees
VIB_PER_DEGREE = 0.1  # vibration rises with load (temperature)
WARMUP = 24  # keep anomalies out of the first day (moving baseline needs history)


def generate_rows(devices: int = 6, days: int = 14, seed: int = 42) -> list[tuple]:
    """Return one tuple per device-hour. Same seed -> same rows."""
    if devices < 2:
        raise ValueError("devices must be >= 2 (the last one is a constant reference sensor)")
    if days < 3:
        raise ValueError("days must be >= 3 (anomalies are kept out of the first day)")
    rng = random.Random(seed)
    hours = days * 24
    rows: list[tuple] = []
    event_id = 0

    for d in range(devices):
        name = f"dev{d + 1:02d}"
        base = 50.0 + 5.0 * d
        vib_base = 1.5 + 0.05 * d
        is_reference = d == devices - 1

        temp, vib, expected, vib_expected = [], [], [], []
        for t in range(hours):
            hour, dow = t % 24, (t // 24) % 7
            weekend = -1.0 if dow >= 5 else 0.0  # machines run a bit cooler on weekends
            exp_t = base + CYCLE * math.sin(2 * math.pi * hour / 24) + weekend
            if is_reference:  # the "reference sensor": a perfectly constant value
                temp.append(60.0)
                vib.append(1.2)
            else:
                value = exp_t + rng.gauss(0, NOISE)
                temp.append(value)
                vib.append(vib_base + VIB_PER_DEGREE * (value - base) + rng.gauss(0, 0.05))
            expected.append(exp_t)
            vib_expected.append(vib_base + VIB_PER_DEGREE * (exp_t - base))

        label = ["normal"] * hours
        event = [-1] * hours

        if not is_reference:
            used: set[int] = set()

            def place(length: int, margin: int = 2) -> int | None:
                for _ in range(500):
                    start = rng.randint(WARMUP, hours - length - margin)
                    span = range(start - margin, start + length + margin)
                    if not any(i in used for i in span):
                        used.update(range(start, start + length))
                        return start
                return None

            # sustained drift: grows from +1.5 to +4 degrees over 4-8 hours
            length = rng.randint(4, 8)
            start = place(length)
            if start is not None:
                event_id += 1
                sign = rng.choice([-1, 1])
                for k in range(length):
                    delta = sign * (1.5 + 2.5 * k / (length - 1))
                    temp[start + k] += delta
                    vib[start + k] += VIB_PER_DEGREE * delta  # load follows temperature
                    label[start + k], event[start + k] = SUSTAINED, event_id

            # combo: temperature a bit high, vibration low (normally they move together)
            for _ in range(max(2, round(0.01 * hours))):
                at = place(1)
                if at is None:
                    continue
                event_id += 1
                sign = rng.choice([-1, 1])
                temp[at] = expected[at] + sign * rng.uniform(2.2, 2.8)
                vib[at] = vib_expected[at] - sign * 0.35 + rng.gauss(0, 0.02)
                label[at], event[at] = COMBO, event_id

            # point anomalies: half strong (6-9 degrees), half weak (2.5-3.5 degrees)
            n_points = max(2, round(0.03 * hours))
            for i in range(n_points):
                at = place(1)
                if at is None:
                    continue
                event_id += 1
                strong = i % 2 == 0
                size = rng.uniform(6, 9) if strong else rng.uniform(2.5, 3.5)
                temp[at] += rng.choice([-1, 1]) * size
                label[at] = POINT_STRONG if strong else POINT_WEAK
                event[at] = event_id

        for t in range(hours):
            rows.append(
                (
                    name,
                    t,
                    t % 24,
                    (t // 24) % 7,
                    round(temp[t], 3),
                    round(vib[t], 3),
                    0 if label[t] == "normal" else 1,
                    label[t],
                    event[t],
                )
            )
    return rows


def generate(spark: SparkSession, devices: int = 6, days: int = 14, seed: int = 42) -> DataFrame:
    """Same as generate_rows, as a Spark DataFrame."""
    return spark.createDataFrame(generate_rows(devices, days, seed), SCHEMA)
