#!/usr/bin/env python3
import argparse
import random
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode


DEFAULT_COUNT = 1_800_000


def build_user(rng: random.Random) -> tuple[str, str]:
    user_id = rng.randint(0, 500)
    username = f"Cornell_{user_id}"
    password = str(user_id) * 10
    return username, password


def build_lat_lon(rng: random.Random) -> tuple[float, float]:
    lat = 38.0235 + (rng.randint(0, 481) - 240.5) / 1000.0
    lon = -122.095 + (rng.randint(0, 325) - 157.0) / 1000.0
    return lat, lon


def format_date(day: int) -> str:
    return date(2015, 4, day).isoformat()


def normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def format_target(method: str, url: str) -> str:
    return f"{method} {url}\n"


def search_hotel(base_url: str, rng: random.Random) -> str:
    in_day = rng.randint(9, 23)
    out_day = rng.randint(in_day + 1, 24)
    lat, lon = build_lat_lon(rng)
    query = urlencode(
        {
            "inDate": format_date(in_day),
            "outDate": format_date(out_day),
            "lat": f"{lat:.4f}",
            "lon": f"{lon:.4f}",
        }
    )
    return format_target("GET", f"{base_url}/hotels?{query}")


def recommend(base_url: str, rng: random.Random) -> str:
    requirement = rng.choice(["dis", "rate", "price"])
    lat, lon = build_lat_lon(rng)
    query = urlencode(
        {
            "require": requirement,
            "lat": f"{lat:.4f}",
            "lon": f"{lon:.4f}",
        }
    )
    return format_target("GET", f"{base_url}/recommendations?{query}")


def user_login(base_url: str, rng: random.Random) -> str:
    username, password = build_user(rng)
    query = urlencode({"username": username, "password": password})
    return format_target("POST", f"{base_url}/user?{query}")


def reserve(base_url: str, rng: random.Random) -> str:
    in_day = rng.randint(9, 23)
    out_day = min(24, in_day + rng.randint(1, 5))
    lat, lon = build_lat_lon(rng)
    hotel_id = rng.randint(1, 80)
    username, password = build_user(rng)
    query = urlencode(
        {
            "inDate": format_date(in_day),
            "outDate": format_date(out_day),
            "lat": f"{lat:.4f}",
            "lon": f"{lon:.4f}",
            "hotelId": hotel_id,
            "customerName": username,
            "username": username,
            "password": password,
            "number": 1,
        }
    )
    return format_target("POST", f"{base_url}/reservation?{query}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a large Vegeta-compatible HotelReservation targets.txt file."
    )
    parser.add_argument("--base-url", default="http://localhost:5000", help="Base URL for the HotelReservation frontend")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of requests to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible generation")
    parser.add_argument("--output", default="targets.txt", help="Output file path")
    parser.add_argument("--search-ratio", type=float, default=0.60, help="Probability of /hotels requests")
    parser.add_argument("--recommend-ratio", type=float, default=0.39, help="Probability of /recommendations requests")
    parser.add_argument("--user-ratio", type=float, default=0.005, help="Probability of /user requests")
    parser.add_argument("--reserve-ratio", type=float, default=0.005, help="Probability of /reservation requests")
    return parser.parse_args()


def build_generator_table(args: argparse.Namespace) -> tuple[list[float], list[str], list[Callable[[str, random.Random], str]]]:
    ratios = {
        "search": args.search_ratio,
        "recommend": args.recommend_ratio,
        "user": args.user_ratio,
        "reserve": args.reserve_ratio,
    }
    total = sum(ratios.values())
    if total <= 0:
        raise ValueError("At least one ratio must be positive")

    # Normalize so small floating-point drift or user-provided weights still work.
    normalized = {name: value / total for name, value in ratios.items()}
    names = ["search", "recommend", "user", "reserve"]
    generators = [search_hotel, recommend, user_login, reserve]

    thresholds = []
    running = 0.0
    for name in names:
        running += normalized[name]
        thresholds.append(running)

    return thresholds, names, generators


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    base_url = normalize_base_url(args.base_url)
    output_path = Path(args.output)

    thresholds, names, generators = build_generator_table(args)
    counts = Counter()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for _ in range(args.count):
            coin = rng.random()
            for threshold, name, generator in zip(thresholds, names, generators):
                if coin < threshold:
                    handle.write(generator(base_url, rng))
                    counts[name] += 1
                    break

    print(f"Wrote {args.count} targets to {output_path}")
    print(f"Seed: {args.seed}")
    for name in names:
        observed = counts[name] / args.count if args.count else 0.0
        print(f"{name}: {counts[name]} ({observed:.4%})")


if __name__ == "__main__":
    main()
