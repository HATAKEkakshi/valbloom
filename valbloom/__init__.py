"""
ValBloom — Redis/Valkey-backed Bloom Filter library.

Usage::

    from valbloom import BloomFilter, CountingBloomFilter, ScalableBloomFilter
"""

__version__ = "1.0.0"

from valbloom.bloom import BloomFilter, CountingBloomFilter, ScalableBloomFilter
from valbloom.exceptions import ValBloomError, IncompatibleFilterError, CapacityExceededError

__all__ = [
    "BloomFilter",
    "CountingBloomFilter",
    "ScalableBloomFilter",
    "ValBloomError",
    "IncompatibleFilterError",
    "CapacityExceededError",
    "__version__",
]
