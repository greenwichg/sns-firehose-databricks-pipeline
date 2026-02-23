#!/usr/bin/env python3
"""Generate realistic test data for the SNS -> Firehose -> S3 -> Databricks pipeline.

Supports two modes:
  1. local  – writes JSON files to a local directory, mirroring the Firehose
               S3 prefix layout so Autoloader can read them directly.
  2. sns    – publishes JSON messages to SNS topics for full end-to-end testing.

Usage:
    # Generate 50 records per topic into ./test_data/
    python scripts/generate_test_data.py --mode local --output-dir ./test_data --records 50

    # Publish 20 records per topic to SNS (requires AWS credentials)
    python scripts/generate_test_data.py --mode sns --records 20 \
        --sns-prefix "batch-pipeline-dev"

    # Generate with a fixed seed for reproducibility
    python scripts/generate_test_data.py --mode local --output-dir ./test_data --seed 42

    # Include 20% invalid records (missing required fields) for validation testing
    python scripts/generate_test_data.py --mode local --output-dir ./test_data \
        --records 100 --invalid-pct 0.2
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import string
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TOPICS = ["orders", "customers", "products"]

CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
    "Toys", "Automotive", "Health", "Beauty", "Food & Beverage",
]
BRANDS = [
    "Acme Corp", "Globex", "Initech", "Umbrella Co", "Wayne Enterprises",
    "Stark Industries", "Cyberdyne", "Soylent Corp", "Wonka Industries", "Aperture Science",
]
STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
]
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer",
    "Michael", "Linda", "David", "Elizabeth", "Sarah", "Daniel",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
]


# ---------------------------------------------------------------------------
# Record generators
# ---------------------------------------------------------------------------
def _random_ts(rng: random.Random, days_back: int = 90) -> datetime:
    """Return a random UTC timestamp within the last ``days_back`` days."""
    offset = timedelta(
        seconds=rng.randint(0, days_back * 86400),
    )
    return datetime.now(timezone.utc) - offset


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_customer(rng: random.Random, *, invalid: bool = False) -> dict:
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    created = _random_ts(rng, days_back=365)
    updated = created + timedelta(days=rng.randint(0, 60))

    record = {
        "customer_id": f"CUST-{uuid.UUID(int=rng.getrandbits(128))}",
        "email": f"{first.lower()}.{last.lower()}{rng.randint(1,999)}@example.com",
        "name": f"{first} {last}",
        "phone": f"+1-{rng.randint(200,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
        "address": f"{rng.randint(1, 9999)} {rng.choice(['Main', 'Oak', 'Elm', 'Park', 'Maple'])} St",
        "city": rng.choice(CITIES),
        "state": rng.choice(US_STATES),
        "country": "US",
        "postal_code": f"{rng.randint(10000, 99999)}",
        "created_at": _iso(created),
        "updated_at": _iso(updated),
        "lifetime_value": round(rng.uniform(0, 50000), 2),
    }

    if invalid:
        # Remove a required field
        del record[rng.choice(["customer_id", "email", "name"])]

    return record


def generate_product(rng: random.Random, *, invalid: bool = False) -> dict:
    adjective = rng.choice(["Premium", "Ultra", "Classic", "Eco", "Pro", "Mini", "Max"])
    noun = rng.choice(["Widget", "Gadget", "Sensor", "Module", "Adapter", "Hub", "Kit"])
    created = _random_ts(rng, days_back=365)
    updated = created + timedelta(days=rng.randint(0, 30))

    record = {
        "product_id": f"PROD-{uuid.UUID(int=rng.getrandbits(128))}",
        "name": f"{adjective} {noun} {rng.choice(string.ascii_uppercase)}{rng.randint(100,999)}",
        "price": round(rng.uniform(1.99, 2999.99), 2),
        "category": rng.choice(CATEGORIES),
        "description": f"High-quality {adjective.lower()} {noun.lower()} for everyday use.",
        "brand": rng.choice(BRANDS),
        "sku": f"SKU-{''.join(rng.choices(string.ascii_uppercase + string.digits, k=8))}",
        "weight": round(rng.uniform(0.1, 50.0), 2),
        "created_at": _iso(created),
        "updated_at": _iso(updated),
    }

    if invalid:
        del record[rng.choice(["product_id", "name", "price"])]

    return record


def generate_order(
    rng: random.Random,
    customer_ids: list[str],
    product_ids: list[str],
    *,
    invalid: bool = False,
) -> dict:
    num_items = rng.randint(1, 5)
    items = []
    total = 0.0
    for _ in range(num_items):
        qty = rng.randint(1, 10)
        price = round(rng.uniform(5.0, 500.0), 2)
        total += qty * price
        items.append({
            "product_id": rng.choice(product_ids) if product_ids else f"PROD-{uuid.uuid4()}",
            "quantity": qty,
            "unit_price": price,
        })

    record = {
        "order_id": f"ORD-{uuid.UUID(int=rng.getrandbits(128))}",
        "customer_id": rng.choice(customer_ids) if customer_ids else f"CUST-{uuid.uuid4()}",
        "order_date": _iso(_random_ts(rng, days_back=90)),
        "total_amount": round(total, 2),
        "status": rng.choice(STATUSES),
        "payment_method": rng.choice(PAYMENT_METHODS),
        "shipping_address_id": f"ADDR-{uuid.UUID(int=rng.getrandbits(128))}",
        "items": items,
    }

    if invalid:
        del record[rng.choice(["order_id", "customer_id", "order_date", "total_amount"])]

    return record


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
def _firehose_prefix(topic: str, ts: datetime) -> str:
    """Replicate the Firehose S3 prefix pattern."""
    return f"{topic}/year={ts:%Y}/month={ts:%m}/day={ts:%d}/hour={ts:%H}"


def write_local(
    records_by_topic: dict[str, list[dict]],
    output_dir: str,
    *,
    compress: bool = True,
) -> None:
    """Write records as JSON files using the Firehose S3 prefix layout.

    Each batch of records is written as a single file (one JSON object per line),
    optionally GZIP-compressed to match real Firehose output.
    """
    base = Path(output_dir)

    for topic, records in records_by_topic.items():
        # Group records by hour bucket to simulate realistic file distribution
        buckets: dict[str, list[dict]] = {}
        for rec in records:
            # Pick a timestamp from the record for partitioning
            ts_field = rec.get("order_date") or rec.get("updated_at") or rec.get("created_at")
            if ts_field:
                ts = datetime.strptime(ts_field, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            else:
                ts = datetime.now(timezone.utc)
            prefix = _firehose_prefix(topic, ts)
            buckets.setdefault(prefix, []).append(rec)

        for prefix, batch in buckets.items():
            dir_path = base / prefix
            dir_path.mkdir(parents=True, exist_ok=True)

            file_id = uuid.uuid4().hex[:12]
            if compress:
                file_path = dir_path / f"{file_id}.json.gz"
                with gzip.open(file_path, "wt", encoding="utf-8") as f:
                    for rec in batch:
                        f.write(json.dumps(rec) + "\n")
            else:
                file_path = dir_path / f"{file_id}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    for rec in batch:
                        f.write(json.dumps(rec) + "\n")

            print(f"  Wrote {len(batch):>4} records -> {file_path}")


def publish_sns(
    records_by_topic: dict[str, list[dict]],
    sns_prefix: str,
    region: str,
) -> None:
    """Publish records to SNS topics for end-to-end pipeline testing."""
    try:
        import boto3
    except ImportError:
        raise SystemExit("boto3 is required for SNS mode: pip install boto3")

    client = boto3.client("sns", region_name=region)

    for topic, records in records_by_topic.items():
        topic_arn = _resolve_topic_arn(client, f"{sns_prefix}-{topic}")
        if not topic_arn:
            print(f"  WARNING: SNS topic '{sns_prefix}-{topic}' not found, skipping")
            continue

        for rec in records:
            client.publish(
                TopicArn=topic_arn,
                Message=json.dumps(rec),
            )
        print(f"  Published {len(records):>4} records -> {topic_arn}")


def _resolve_topic_arn(client, topic_name: str) -> str | None:
    """Find a topic ARN by name suffix."""
    paginator = client.get_paginator("list_topics")
    for page in paginator.paginate():
        for t in page["Topics"]:
            if t["TopicArn"].endswith(f":{topic_name}"):
                return t["TopicArn"]
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def generate_all(
    num_records: int,
    invalid_pct: float,
    seed: int | None,
) -> dict[str, list[dict]]:
    """Generate records for all topics and return them grouped by topic."""
    rng = random.Random(seed)

    num_invalid = int(num_records * invalid_pct)
    num_valid = num_records - num_invalid

    # Generate customers and products first so orders can reference their IDs
    customers = [generate_customer(rng) for _ in range(num_valid)]
    customers += [generate_customer(rng, invalid=True) for _ in range(num_invalid)]
    rng.shuffle(customers)

    products = [generate_product(rng) for _ in range(num_valid)]
    products += [generate_product(rng, invalid=True) for _ in range(num_invalid)]
    rng.shuffle(products)

    customer_ids = [c["customer_id"] for c in customers if "customer_id" in c]
    product_ids = [p["product_id"] for p in products if "product_id" in p]

    orders = [generate_order(rng, customer_ids, product_ids) for _ in range(num_valid)]
    orders += [
        generate_order(rng, customer_ids, product_ids, invalid=True)
        for _ in range(num_invalid)
    ]
    rng.shuffle(orders)

    return {
        "orders": orders,
        "customers": customers,
        "products": products,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate test data for the batch processing pipeline.",
    )
    parser.add_argument(
        "--mode",
        choices=["local", "sns"],
        default="local",
        help="Output mode (default: local)",
    )
    parser.add_argument(
        "--output-dir",
        default="./test_data",
        help="Local output directory (default: ./test_data)",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=50,
        help="Number of records per topic (default: 50)",
    )
    parser.add_argument(
        "--invalid-pct",
        type=float,
        default=0.1,
        help="Fraction of records with missing required fields (default: 0.1)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Write plain JSON instead of GZIP (local mode only)",
    )
    parser.add_argument(
        "--sns-prefix",
        default="batch-pipeline-dev",
        help="SNS topic name prefix (sns mode only)",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region for SNS (default: us-east-1)",
    )

    args = parser.parse_args()

    print(f"Generating {args.records} records per topic ({args.invalid_pct:.0%} invalid)...")
    if args.seed is not None:
        print(f"Using seed: {args.seed}")

    records = generate_all(args.records, args.invalid_pct, args.seed)

    total = sum(len(v) for v in records.values())
    print(f"Generated {total} total records across {len(TOPICS)} topics\n")

    if args.mode == "local":
        print(f"Writing to {args.output_dir}/ (compress={not args.no_compress}):")
        write_local(records, args.output_dir, compress=not args.no_compress)
    else:
        print(f"Publishing to SNS (prefix={args.sns_prefix}, region={args.region}):")
        publish_sns(records, args.sns_prefix, args.region)

    print("\nDone.")


if __name__ == "__main__":
    main()
