#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import re


FACTORS_MS = {
    "ns": 0.000001,
    "us": 0.001,
    "ms": 1.0,
    "s": 1000.0,
    "m": 60000.0,
    "h": 3600000.0,
}


def parse_time_ms(raw: str) -> float:
    match = re.fullmatch(r"\s*([+-]?\d+(?:\.\d+)?)([a-zA-Z]+)?\s*", raw)
    if not match:
        raise ValueError(f"unsupported time value: {raw}")

    value = float(match.group(1))
    unit = match.group(2) or "ms"
    if unit not in FACTORS_MS:
        raise ValueError(f"unsupported time unit in {raw}")
    return value * FACTORS_MS[unit]


def format_ms(value: float) -> str:
    if value < 0:
        raise ValueError("netem delay cannot be negative")
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value))}ms"
    return f"{value:.3f}".rstrip("0").rstrip(".") + "ms"


def add_values(left: str, right: str) -> None:
    print(format_ms(parse_time_ms(left) + parse_time_ms(right)))


def subtract_values(left: str, right: str) -> None:
    print(format_ms(max(0.0, parse_time_ms(left) - parse_time_ms(right))))


def ramp_delays_by_increment(start_ms: float, end_ms: float, increment_ms: float) -> list[float]:
    if increment_ms <= 0:
        raise ValueError("ramp increment must be positive")

    if math.isclose(start_ms, end_ms):
        return [start_ms]

    direction = 1.0 if end_ms > start_ms else -1.0
    delays = [start_ms]
    current = start_ms

    while True:
        next_delay = current + (direction * increment_ms)
        if (direction > 0 and next_delay >= end_ms) or (direction < 0 and next_delay <= end_ms):
            break
        delays.append(next_delay)
        current = next_delay

    if not math.isclose(delays[-1], end_ms):
        delays.append(end_ms)
    return delays


def ramp_values(start: str, end: str, duration: str, step_spec: str) -> None:
    start_ms = parse_time_ms(start)
    end_ms = parse_time_ms(end)
    duration_s = parse_time_ms(duration) / 1000.0

    if re.fullmatch(r"\d+", step_spec.strip()):
        steps = max(1, int(step_spec))
        delays = [
            start_ms + ((end_ms - start_ms) * (index / steps))
            for index in range(steps + 1)
        ]
    else:
        delays = ramp_delays_by_increment(start_ms, end_ms, parse_time_ms(step_spec))

    intervals = max(1, len(delays) - 1)
    interval_s = duration_s / intervals if delays else 0.0

    for index, delay_ms in enumerate(delays):
        sleep_s = interval_s if index < len(delays) - 1 else 0.0
        print(f"{format_ms(delay_ms)} {sleep_s:.6f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Time-value helpers for eval shell scripts.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add", help="Add two time values and print milliseconds.")
    add_parser.add_argument("left")
    add_parser.add_argument("right")

    subtract_parser = subparsers.add_parser(
        "subtract",
        help="Subtract two time values, clamped at zero, and print milliseconds.",
    )
    subtract_parser.add_argument("left")
    subtract_parser.add_argument("right")

    ramp_parser = subparsers.add_parser("ramp", help="Print netem delay/sleep steps.")
    ramp_parser.add_argument("start")
    ramp_parser.add_argument("end")
    ramp_parser.add_argument("duration")
    ramp_parser.add_argument("step_spec")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "add":
        add_values(args.left, args.right)
    elif args.command == "subtract":
        subtract_values(args.left, args.right)
    elif args.command == "ramp":
        ramp_values(args.start, args.end, args.duration, args.step_spec)
    else:
        raise AssertionError(f"unexpected command: {args.command}")


if __name__ == "__main__":
    main()
