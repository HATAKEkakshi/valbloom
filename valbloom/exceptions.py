"""
ValBloom custom exceptions.
"""


class ValBloomError(Exception):
    """Base exception for all ValBloom errors."""
    pass


class IncompatibleFilterError(ValBloomError):
    """
    Raised when attempting to union or intersect bloom filters
    that have different sizes (m) or hash counts (k).
    """
    pass


class CapacityExceededError(ValBloomError):
    """
    Raised when a standard BloomFilter exceeds its designed capacity.

    Beyond this point the actual false-positive rate will be higher
    than the configured ``error_rate``.
    """
    pass
