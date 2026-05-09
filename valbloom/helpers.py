from __future__ import annotations

import math
import hashlib
from typing import List

"""
Internal helper utilities for ValBloom bloom filter calculations.

This module contains the core mathematical and hashing functions used by
all bloom filter variants. These are internal implementation details and
should not be imported directly by library consumers.
"""


def optimal_m(capacity: int, error_rate: float) -> int:
    """
    Calculate the optimal bit-array size (m) for a bloom filter.

    Uses the standard formula derived from the desired false-positive
    probability and expected number of elements:

        m = -(n * ln(p)) / (ln(2))^2

    where:
        n = expected number of elements (capacity)
        p = desired false-positive probability (error_rate)
        m = number of bits in the filter

    Args:
        capacity: Expected number of unique elements to be inserted (n).
        error_rate: Target false-positive probability, e.g. 0.001 for 0.1%.

    Returns:
        The optimal number of bits for the filter's bit-array.

    Example:
        >>> optimal_m(capacity=100_000, error_rate=0.001)
        1437759
    """
    return int(-(capacity * math.log(error_rate)) / (math.log(2) ** 2))


def optimal_k(m: int, capacity: int) -> int:
    """
    Calculate the optimal number of hash functions (k) for a bloom filter.

    Uses the standard formula:

        k = (m / n) * ln(2)

    where:
        m = number of bits in the filter
        n = expected number of elements (capacity)
        k = number of independent hash functions

    A minimum of 1 hash function is always enforced.

    Args:
        m: Size of the bit-array in bits.
        capacity: Expected number of unique elements to be inserted (n).

    Returns:
        The optimal number of hash functions. Always >= 1.

    Example:
        >>> optimal_k(m=1437759, capacity=100_000)
        10
    """
    return max(1, int((m / capacity) * math.log(2)))


def hash_offsets(item: str, k: int, m: int) -> List[int]:
    """
    Generate k deterministic bit positions for the given item.

    Each hash is produced by appending a unique salt index to the item
    string and computing a SHA-256 digest. The resulting hex digest is
    converted to an integer and reduced modulo m to produce a valid
    bit offset within the filter's bit-array.

    The hashing scheme is: SHA256("{item}:{i}") for i in 0..k-1

    SHA-256 is chosen over faster non-cryptographic hashes (e.g. MurmurHash)
    because it requires no external dependencies while providing excellent
    uniformity. The performance trade-off is acceptable since Redis network
    round-trip latency dominates overall operation time.

    Args:
        item: The string element to hash.
        k: Number of hash functions (bit positions to generate).
        m: Size of the bit-array in bits (offsets will be in range [0, m)).

    Returns:
        A list of k integer offsets, each in the range [0, m).

    Example:
        >>> hash_offsets("alice@example.com", k=10, m=1437759)
        [839421, 102834, ...]  # 10 deterministic offsets
    """
    offsets: List[int] = []
    for i in range(k):
        digest = hashlib.sha256(f"{item}:{i}".encode()).hexdigest()
        offsets.append(int(digest, 16) % m)
    return offsets


def validate_params(capacity: int, error_rate: float) -> None:
    """
    Validate bloom filter constructor parameters.

    Ensures that the capacity is a positive integer and the error rate
    is a float strictly between 0 and 1 (exclusive on both ends).
    An error_rate of 0 would require infinite bits, and an error_rate
    of 1 would make the filter useless.

    Args:
        capacity: Expected number of elements. Must be a positive integer.
        error_rate: Target false-positive probability. Must satisfy 0 < p < 1.

    Raises:
        ValueError: If capacity is not a positive integer.
        ValueError: If error_rate is not strictly between 0 and 1.
    """
    if not isinstance(capacity, int) or capacity <= 0:
        raise ValueError(f"capacity must be a positive integer, got {capacity!r}")
    if not (0 < error_rate < 1):
        raise ValueError(f"error_rate must be between 0 and 1 exclusive, got {error_rate!r}")
