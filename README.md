# ValBloom

[![PyPI version](https://img.shields.io/pypi/v/valbloom.svg)](https://pypi.org/project/valbloom/)
[![Python versions](https://img.shields.io/pypi/pyversions/valbloom.svg)](https://pypi.org/project/valbloom/)
[![License: MIT](https://img.shields.io/pypi/l/valbloom.svg)](https://github.com/HATAKEkakshi/valbloom/blob/main/LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/valbloom.svg)](https://pypi.org/project/valbloom/)

Fast, memory-efficient **Bloom Filters** backed by **Redis / Valkey** bitmaps.

ValBloom provides three async-first probabilistic data structures for membership testing — ideal for token blacklisting, duplicate detection, rate limiting, and more.

## Installation

```bash
pip install valbloom
```

> Requires Python ≥ 3.9 and a running Redis or Valkey instance.

## Quick Start

```python
from redis.asyncio import Redis
from valbloom import BloomFilter

r = Redis.from_url("redis://localhost")

# Create a filter expecting up to 100k items with 0.1% false-positive rate
bf = BloomFilter(r, "bf:emails", capacity=100_000, error_rate=0.001)

await bf.add("alice@example.com")
await bf.exists("alice@example.com")   # True
await bf.exists("bob@example.com")     # False (probably)
```

## Filter Types

### `BloomFilter` — Standard

The classic space-efficient probabilistic set. Supports **add** and **membership testing** but not deletion.

```python
bf = BloomFilter(client, "bf:tokens", capacity=100_000)

await bf.add("token-abc")
await bf.add_many(["token-1", "token-2", "token-3"])

await bf.exists("token-abc")               # True
await bf.exists_many(["token-1", "nope"])   # {"token-1": True, "nope": False}

# Set operations (requires compatible filters — same capacity & error_rate)
merged = await bf1.union(bf2, dest_key="bf:merged")
common = await bf1.intersection(bf2, dest_key="bf:common")
```

### `CountingBloomFilter` — Supports Deletion

Uses counter buckets (Redis hash) instead of single bits, allowing items to be **removed**.

```python
from valbloom import CountingBloomFilter

cbf = CountingBloomFilter(client, "cbf:sessions", capacity=50_000)

await cbf.add("session-xyz")
await cbf.exists("session-xyz")   # True

await cbf.remove("session-xyz")
await cbf.exists("session-xyz")   # False
```

### `ScalableBloomFilter` — Auto-Growing

Automatically creates new filter slices when the current one fills up, so the false-positive rate stays bounded even as data grows beyond the initial capacity.

```python
from valbloom import ScalableBloomFilter

sbf = ScalableBloomFilter(
    client, "sbf:users",
    initial_capacity=1_000,
    growth_factor=2,  # each new slice doubles in capacity
    ratio=0.9,        # each slice tightens the error rate
)

# Add millions of items — ValBloom handles the scaling
for i in range(50_000):
    await sbf.add(f"user-{i}")
```

## Common API

All three filter types share these methods:

| Method | Description |
|---|---|
| `add(item)` | Add a single item |
| `exists(item) → bool` | Check membership (probabilistic) |
| `add_many(items)` | Bulk add |
| `exists_many(items) → dict` | Bulk check |
| `count → int` | Number of items added |
| `info() → dict` | Filter stats (size, fill ratio, etc.) |
| `clear()` | Reset the filter |
| `set_ttl(seconds)` | Set expiration on Redis keys |
| `get_ttl() → int` | Get remaining TTL |

`CountingBloomFilter` additionally supports:

| Method | Description |
|---|---|
| `remove(item)` | Remove a single item |
| `remove_many(items)` | Bulk remove |

`BloomFilter` additionally supports:

| Method | Description |
|---|---|
| `union(other, dest_key)` | Merge filters with `BITOP OR` |
| `intersection(other, dest_key)` | Intersect filters with `BITOP AND` |

## How It Works

A Bloom filter uses a **bit array** of *m* bits and *k* independent hash functions. When adding an item, all *k* hash positions are set to 1. To check membership, all *k* positions are tested — if any is 0, the item is definitely not in the set.

ValBloom automatically computes optimal *m* and *k* from your desired `capacity` and `error_rate`:

- **Bit-array size**: `m = -(n × ln p) / (ln 2)²`
- **Hash count**: `k = (m / n) × ln 2`

| Capacity | Error Rate | Memory | Hashes |
|---|---|---|---|
| 10,000 | 1% | ~12 KB | 7 |
| 100,000 | 0.1% | ~180 KB | 10 |
| 1,000,000 | 0.01% | ~2.3 MB | 13 |

## When to Use Which

| Use case | Filter |
|---|---|
| Token blacklisting, email dedup, IP blocklisting | `BloomFilter` |
| Session tracking, temporary bans (need removal) | `CountingBloomFilter` |
| Unbounded growth, don't know final size | `ScalableBloomFilter` |

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests (uses fakeredis, no real Redis needed)
pytest tests/ -v
```

## License

MIT
