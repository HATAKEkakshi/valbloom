from __future__ import annotations

import logging
import math
from typing import Iterable, Dict, Any, List, Optional

from redis.asyncio import Redis

from valbloom.helpers import optimal_m, optimal_k, hash_offsets, validate_params
from valbloom.exceptions import IncompatibleFilterError

logger = logging.getLogger(__name__)

"""
ValBloom — Redis/Valkey-backed Bloom Filter implementations.

Provides three probabilistic data structures for fast, memory-efficient
membership testing using Redis as the backing store:

- ``BloomFilter``          — Standard bloom filter.
- ``CountingBloomFilter``  — Supports deletions via counter buckets.
- ``ScalableBloomFilter``  — Auto-grows when capacity is reached.
"""


class BloomFilter:
    """
    Standard Bloom Filter backed by a Redis bitmap.

    Uses a fixed-size bit-array stored as a Redis string key. Items are
    mapped to ``k`` bit positions via SHA-256 hashing. Once a bit is set
    it can never be unset, so this filter does **not** support deletion.

    False positives are possible (the filter may report an item is present
    when it is not), but false negatives are impossible (if the filter says
    an item is absent, it is guaranteed absent).

    Redis Keys Used:
        - ``{key}``        — The bitmap itself.
        - ``{key}:count``  — An integer counter tracking how many items
                             have been added (used by ``count`` and ``info``).

    Args:
        client: An async ``redis.asyncio.Redis`` or Valkey client instance.
        key: The Redis key name to store the bitmap under.
        capacity: Maximum number of elements the filter is designed for (n).
            Exceeding this will degrade the false-positive rate beyond
            the configured ``error_rate``.
        error_rate: Target false-positive probability (p). Must be between
            0 and 1 exclusive. Default is ``0.001`` (0.1%).

    Raises:
        ValueError: If ``capacity`` is not a positive integer or
            ``error_rate`` is not in the open interval (0, 1).

    Example::

        from redis.asyncio import Redis
        from valbloom import BloomFilter

        r = Redis.from_url("redis://localhost")
        bf = BloomFilter(r, "bf:emails", capacity=100_000)

        await bf.add("alice@example.com")
        assert await bf.exists("alice@example.com")   # True
        assert not await bf.exists("bob@example.com")  # (probably) False
    """

    __slots__ = ("client", "key", "capacity", "error_rate", "m", "k", "_count_key")

    def __init__(
        self,
        client: Redis,
        key: str,
        capacity: int,
        error_rate: float = 0.001,
    ) -> None:
        validate_params(capacity, error_rate)
        self.client = client
        self.key = key
        self.capacity = capacity
        self.error_rate = error_rate
        self.m = optimal_m(capacity, error_rate)
        self.k = optimal_k(self.m, capacity)
        self._count_key = f"{key}:count"

    async def add(self, item: str) -> None:
        """
        Add a single item to the bloom filter.

        Computes ``k`` hash positions for the item and sets each
        corresponding bit to 1 in the Redis bitmap. Also increments
        the internal counter by 1.

        This operation is idempotent for the bitmap (adding the same
        item twice does not change the bit-array), but the counter
        will still increment — it tracks total ``add()`` calls, not
        unique items.

        Args:
            item: The string element to insert into the filter.
        """
        pipe = self.client.pipeline()
        for offset in hash_offsets(item, self.k, self.m):
            pipe.setbit(self.key, offset, 1)
        pipe.incr(self._count_key)
        await pipe.execute()

    async def exists(self, item: str) -> bool:
        """
        Check whether an item **might** exist in the filter.

        Computes the same ``k`` hash positions used by ``add()`` and
        checks whether every corresponding bit is set to 1. If any
        bit is 0, the item is **definitely** not in the filter. If all
        bits are 1, the item is **probably** in the filter (subject to
        the configured false-positive rate).

        Args:
            item: The string element to look up.

        Returns:
            ``True`` if the item is probably present (all k bits are set).
            ``False`` if the item is definitely absent (at least one bit is 0).
        """
        pipe = self.client.pipeline()
        for offset in hash_offsets(item, self.k, self.m):
            pipe.getbit(self.key, offset)
        results = await pipe.execute()
        return all(results)

    async def add_many(self, items: Iterable[str]) -> None:
        """
        Add multiple items in a single Redis pipeline round-trip.

        All ``SETBIT`` commands for every item are batched into one
        pipeline, followed by a single ``INCRBY`` for the counter.
        This is significantly faster than calling ``add()`` in a loop
        when inserting large batches.

        Args:
            items: An iterable of string elements to insert.
        """
        pipe = self.client.pipeline()
        n = 0
        for item in items:
            for offset in hash_offsets(item, self.k, self.m):
                pipe.setbit(self.key, offset, 1)
            n += 1
        if n:
            pipe.incrby(self._count_key, n)
            await pipe.execute()

    async def exists_many(self, items: Iterable[str]) -> Dict[str, bool]:
        """
        Check multiple items for membership in a single pipeline round-trip.

        All ``GETBIT`` commands are batched together. The results are then
        sliced into groups of ``k`` bits per item to determine membership.

        Args:
            items: An iterable of string elements to check.

        Returns:
            A dict mapping each input item to its membership result.
            Example: ``{"alice": True, "bob": False, "charlie": True}``
        """
        items_list = list(items)
        pipe = self.client.pipeline()
        for item in items_list:
            for offset in hash_offsets(item, self.k, self.m):
                pipe.getbit(self.key, offset)
        results = await pipe.execute()

        out: Dict[str, bool] = {}
        idx = 0
        for item in items_list:
            bits = results[idx : idx + self.k]
            out[item] = all(bits)
            idx += self.k
        return out

    @property
    async def count(self) -> int:
        """
        Number of items that have been added to the filter.

        This is the cumulative count of ``add()`` / ``add_many()`` calls,
        not the number of unique items. Stored as a separate Redis key
        (``{key}:count``) and incremented atomically on each add.

        Returns:
            The total number of add operations performed.
        """
        val = await self.client.get(self._count_key)
        return int(val) if val else 0

    async def info(self) -> Dict[str, Any]:
        """
        Return a dictionary of filter configuration and live statistics.

        Returns:
            A dict with the following keys:

            - **key** (``str``): The Redis key storing the bitmap.
            - **capacity** (``int``): Designed maximum number of elements.
            - **error_rate** (``float``): Target false-positive probability.
            - **size_bits** (``int``): Total size of the bit-array in bits (m).
            - **num_hashes** (``int``): Number of hash functions used (k).
            - **count** (``int``): Number of items added so far.
            - **fill_ratio** (``float``): Fraction of bits set to 1
              (0.0 = empty, 1.0 = fully saturated).
            - **size_bytes** (``int``): Approximate memory usage in bytes.
        """
        cnt = await self.count
        bits_set = await self.client.bitcount(self.key)
        return {
            "key": self.key,
            "capacity": self.capacity,
            "error_rate": self.error_rate,
            "size_bits": self.m,
            "num_hashes": self.k,
            "count": cnt,
            "fill_ratio": bits_set / self.m if self.m else 0.0,
            "size_bytes": math.ceil(self.m / 8),
        }

    async def clear(self) -> None:
        """
        Delete the bloom filter bitmap and its counter from Redis.

        After calling this, the filter is empty and ``count`` returns 0.
        The ``BloomFilter`` instance remains usable — new items can be
        added immediately.
        """
        await self.client.delete(self.key, self._count_key)

    async def set_ttl(self, seconds: int) -> None:
        """
        Set a time-to-live (expiration) on the bitmap and counter keys.

        Both the bitmap key and the counter key are given the same TTL
        so they expire together. Useful for temporary filters like
        rate-limiting windows or session-scoped checks.

        Args:
            seconds: Number of seconds until the keys expire.
        """
        pipe = self.client.pipeline()
        pipe.expire(self.key, seconds)
        pipe.expire(self._count_key, seconds)
        await pipe.execute()

    async def get_ttl(self) -> int:
        """
        Return the remaining time-to-live of the bitmap key.

        Returns:
            Seconds remaining. ``-1`` means no expiry is set.
            ``-2`` means the key does not exist.
        """
        return await self.client.ttl(self.key)

    async def union(self, other: "BloomFilter", dest_key: Optional[str] = None) -> "BloomFilter":
        """
        Merge this filter with another using bitwise OR (``BITOP OR``).

        The result contains all items from both filters. Both filters
        must have identical ``capacity`` and ``error_rate`` (which
        determines matching ``m`` and ``k``).

        Args:
            other: Another ``BloomFilter`` with matching ``capacity``
                and ``error_rate``.
            dest_key: Redis key for the merged result. Defaults to
                ``self.key`` (in-place merge).

        Returns:
            A new ``BloomFilter`` instance pointing at the destination key.

        Raises:
            IncompatibleFilterError: If ``capacity`` or ``error_rate``
                differ between the two filters.
        """
        self._assert_compatible(other)
        target = dest_key or self.key
        await self.client.bitop("OR", target, self.key, other.key)
        bf = BloomFilter.__new__(BloomFilter)
        bf.client, bf.key, bf.capacity = self.client, target, self.capacity
        bf.error_rate, bf.m, bf.k = self.error_rate, self.m, self.k
        bf._count_key = f"{target}:count"
        return bf

    async def intersection(self, other: "BloomFilter", dest_key: Optional[str] = None) -> "BloomFilter":
        """
        Intersect this filter with another using bitwise AND (``BITOP AND``).

        The result approximates items that are in **both** filters.
        Both filters must have identical ``capacity`` and ``error_rate``.

        Args:
            other: Another ``BloomFilter`` with matching ``capacity``
                and ``error_rate``.
            dest_key: Redis key for the result. Defaults to ``self.key``.

        Returns:
            A new ``BloomFilter`` instance pointing at the destination key.

        Raises:
            IncompatibleFilterError: If ``capacity`` or ``error_rate``
                differ between the two filters.
        """
        self._assert_compatible(other)
        target = dest_key or self.key
        await self.client.bitop("AND", target, self.key, other.key)
        bf = BloomFilter.__new__(BloomFilter)
        bf.client, bf.key, bf.capacity = self.client, target, self.capacity
        bf.error_rate, bf.m, bf.k = self.error_rate, self.m, self.k
        bf._count_key = f"{target}:count"
        return bf

    def _assert_compatible(self, other: "BloomFilter") -> None:
        """
        Raise ``IncompatibleFilterError`` if the two filters were created
        with different ``capacity`` or ``error_rate``.

        The check compares the derived ``m`` (bit-array size) and ``k``
        (hash count), which are deterministic functions of ``capacity``
        and ``error_rate``.
        """
        if self.m != other.m or self.k != other.k:
            raise IncompatibleFilterError(
                f"Filters must have matching capacity and error_rate. "
                f"Got capacity={self.capacity}/error_rate={self.error_rate} "
                f"(m={self.m}, k={self.k}) vs "
                f"capacity={other.capacity}/error_rate={other.error_rate} "
                f"(m={other.m}, k={other.k})"
            )

    def __repr__(self) -> str:
        return (
            f"BloomFilter(key={self.key!r}, capacity={self.capacity}, "
            f"error_rate={self.error_rate}, m={self.m}, k={self.k})"
        )


class CountingBloomFilter:
    """
    Counting Bloom Filter backed by a Redis hash.

    Instead of single bits, each bucket is an integer counter stored as
    a field in a Redis hash. This allows items to be **removed** by
    decrementing the counters, which is impossible with a standard
    ``BloomFilter``.

    Trade-off: Uses significantly more memory than a standard bloom filter
    (one hash field per active bucket vs. one bit), but enables deletion.

    Redis Keys Used:
        - ``{key}``        — A Redis hash where field names are bucket
                             offsets and values are counter integers.
        - ``{key}:count``  — An integer tracking net item count.

    Args:
        client: An async ``redis.asyncio.Redis`` or Valkey client instance.
        key: The Redis key name for the hash.
        capacity: Maximum number of elements the filter is designed for.
        error_rate: Target false-positive probability. Default ``0.001``.

    Raises:
        ValueError: If ``capacity`` or ``error_rate`` are invalid.

    Example::

        cbf = CountingBloomFilter(r, "cbf:sessions", capacity=50_000)
        await cbf.add("session-abc")
        await cbf.remove("session-abc")
        assert not await cbf.exists("session-abc")
    """

    __slots__ = ("client", "key", "capacity", "error_rate", "m", "k", "_count_key")

    def __init__(self, client: Redis, key: str, capacity: int, error_rate: float = 0.001) -> None:
        validate_params(capacity, error_rate)
        self.client = client
        self.key = key
        self.capacity = capacity
        self.error_rate = error_rate
        self.m = optimal_m(capacity, error_rate)
        self.k = optimal_k(self.m, capacity)
        self._count_key = f"{key}:count"

    async def add(self, item: str) -> None:
        """
        Add an item by incrementing its ``k`` counter buckets by 1.

        .. warning:: Duplicate-add behaviour

            Adding the same item **N** times requires **N** corresponding
            ``remove()`` calls before ``exists()`` returns ``False``.
            A ``CountingBloomFilter`` does **not** de-duplicate; each
            ``add()`` is an independent counter increment.

            Example::

                await cbf.add("session-xyz")
                await cbf.add("session-xyz")   # counter = 2
                await cbf.remove("session-xyz") # counter = 1
                await cbf.exists("session-xyz") # True  (still 1 add outstanding)

        If the item appears to already be present in the filter (all
        ``k`` buckets are already > 0), a ``WARNING``-level log message
        is emitted.  This does **not** prevent the add — callers who
        need strict single-add semantics should gate on ``exists()``
        before calling ``add()``.

        Args:
            item: The string element to insert.
        """
        offsets = hash_offsets(item, self.k, self.m)

        # Pre-read buckets to detect a probable duplicate
        pipe = self.client.pipeline()
        for offset in offsets:
            pipe.hget(self.key, str(offset))
        values = await pipe.execute()

        if all(v is not None and int(v) > 0 for v in values):
            logger.warning(
                "Item %r appears to already exist in CountingBloomFilter(%s). "
                "Adding again — remember that each add() requires a "
                "matching remove() before exists() returns False.",
                item,
                self.key,
            )

        pipe = self.client.pipeline()
        for offset in offsets:
            pipe.hincrby(self.key, str(offset), 1)
        pipe.incr(self._count_key)
        await pipe.execute()

    async def remove(self, item: str) -> None:
        """
        Remove an item by decrementing its ``k`` counter buckets.

        First reads all bucket values. Only decrements if **every**
        bucket has a counter > 0, preventing underflow that would
        corrupt the filter. If any bucket is already 0, the item is
        considered absent and no changes are made.

        .. note::

            If an item was added **N** times, it must be removed **N**
            times before ``exists()`` returns ``False``. A single
            ``remove()`` only decrements the counters by 1.

        Args:
            item: The string element to remove.
        """
        offsets = hash_offsets(item, self.k, self.m)
        pipe = self.client.pipeline()
        for offset in offsets:
            pipe.hget(self.key, str(offset))
        values = await pipe.execute()

        if all(v is not None and int(v) > 0 for v in values):
            pipe = self.client.pipeline()
            for offset in offsets:
                pipe.hincrby(self.key, str(offset), -1)
            pipe.decr(self._count_key)
            await pipe.execute()

    async def exists(self, item: str) -> bool:
        """
        Check whether an item might be in the filter.

        Returns ``True`` only if all ``k`` counter buckets are > 0.

        Args:
            item: The string element to look up.

        Returns:
            ``True`` if probably present, ``False`` if definitely absent.
        """
        pipe = self.client.pipeline()
        for offset in hash_offsets(item, self.k, self.m):
            pipe.hget(self.key, str(offset))
        values = await pipe.execute()
        return all(v is not None and int(v) > 0 for v in values)

    async def add_many(self, items: Iterable[str]) -> None:
        """
        Add multiple items in a single Redis pipeline.

        Args:
            items: An iterable of string elements to insert.
        """
        pipe = self.client.pipeline()
        n = 0
        for item in items:
            for offset in hash_offsets(item, self.k, self.m):
                pipe.hincrby(self.key, str(offset), 1)
            n += 1
        if n:
            pipe.incrby(self._count_key, n)
            await pipe.execute()

    async def remove_many(self, items: Iterable[str]) -> None:
        """
        Remove multiple items. Items not present are silently skipped.

        Args:
            items: An iterable of string elements to remove.
        """
        for item in items:
            await self.remove(item)

    async def exists_many(self, items: Iterable[str]) -> Dict[str, bool]:
        """
        Check multiple items in a single pipeline.

        Args:
            items: An iterable of string elements to check.

        Returns:
            A dict mapping each item to ``True`` (probably present)
            or ``False`` (definitely absent).
        """
        items_list = list(items)
        pipe = self.client.pipeline()
        for item in items_list:
            for offset in hash_offsets(item, self.k, self.m):
                pipe.hget(self.key, str(offset))
        results = await pipe.execute()

        out: Dict[str, bool] = {}
        idx = 0
        for item in items_list:
            vals = results[idx : idx + self.k]
            out[item] = all(v is not None and int(v) > 0 for v in vals)
            idx += self.k
        return out

    @property
    async def count(self) -> int:
        """Net item count (adds minus removes)."""
        val = await self.client.get(self._count_key)
        return int(val) if val else 0

    async def info(self) -> Dict[str, Any]:
        """
        Return filter configuration and live statistics.

        Returns:
            A dict with the following keys:

            - **key** (``str``): Redis key for the hash.
            - **capacity** (``int``): Designed maximum elements.
            - **error_rate** (``float``): Target false-positive probability.
            - **size_buckets** (``int``): Total number of possible buckets (m).
            - **num_hashes** (``int``): Hash functions per item (k).
            - **count** (``int``): Net items (adds minus removes).
            - **active_buckets** (``int``): Number of hash fields with
              non-zero counters currently stored in Redis.
        """
        cnt = await self.count
        num_buckets = await self.client.hlen(self.key)
        return {
            "key": self.key,
            "capacity": self.capacity,
            "error_rate": self.error_rate,
            "size_buckets": self.m,
            "num_hashes": self.k,
            "count": cnt,
            "active_buckets": num_buckets,
        }

    async def clear(self) -> None:
        """Delete the filter hash and counter from Redis."""
        await self.client.delete(self.key, self._count_key)

    async def set_ttl(self, seconds: int) -> None:
        """Set expiration on the hash and counter keys."""
        pipe = self.client.pipeline()
        pipe.expire(self.key, seconds)
        pipe.expire(self._count_key, seconds)
        await pipe.execute()

    async def get_ttl(self) -> int:
        """Return remaining TTL in seconds (-1 = no expiry, -2 = missing)."""
        return await self.client.ttl(self.key)

    def __repr__(self) -> str:
        return (
            f"CountingBloomFilter(key={self.key!r}, capacity={self.capacity}, "
            f"error_rate={self.error_rate}, m={self.m}, k={self.k})"
        )


class ScalableBloomFilter:
    """
    Scalable Bloom Filter that auto-grows by creating new filter slices
    when the current slice reaches its capacity.

    Internally manages a chain of ``BloomFilter`` instances stored at
    ``{key}:0``, ``{key}:1``, etc. When the active slice fills up, a
    new slice is created with increased capacity (multiplied by
    ``growth_factor``) and a tighter error rate (multiplied by ``ratio``)
    so the overall false-positive probability converges to the initial target.

    **TTL behaviour:**
        When ``set_ttl(seconds)`` is called, the TTL is stored in the
        metadata hash and applied to **all existing slice keys**.  When
        the filter auto-scales and creates a new slice, the stored TTL
        is automatically inherited by the new slice so that all keys
        expire together. If no TTL has been set, new slices are created
        without an expiration.

    Redis Keys Used:
        - ``{key}:N``        — Bitmap for slice N.
        - ``{key}:N:count``  — Counter for slice N.
        - ``{key}:meta``     — Hash storing ``num_slices`` and ``ttl``.

    Args:
        client: An async ``redis.asyncio.Redis`` or Valkey client instance.
        key: Base Redis key. Slices are stored as ``{key}:0``, ``{key}:1``, etc.
        initial_capacity: Capacity of the first slice.
        error_rate: Overall target false-positive probability. Default ``0.001``.
        growth_factor: Capacity multiplier for each new slice. Default ``2``.
        ratio: Error-rate tightening ratio per slice. Default ``0.9``.

    Raises:
        ValueError: If parameters are out of valid ranges.

    Example::

        sbf = ScalableBloomFilter(r, "sbf:users", initial_capacity=1000)
        for i in range(5000):
            await sbf.add(f"user-{i}")
        # Internally creates multiple slices to stay within error bounds
    """

    __slots__ = (
        "client", "key", "initial_capacity", "error_rate",
        "growth_factor", "ratio", "_meta_key",
    )

    def __init__(
        self, client: Redis, key: str, initial_capacity: int,
        error_rate: float = 0.001, growth_factor: int = 2, ratio: float = 0.9,
    ) -> None:
        validate_params(initial_capacity, error_rate)
        if growth_factor < 1:
            raise ValueError(f"growth_factor must be >= 1, got {growth_factor!r}")
        if not (0 < ratio < 1):
            raise ValueError(f"ratio must be between 0 and 1 exclusive, got {ratio!r}")
        self.client = client
        self.key = key
        self.initial_capacity = initial_capacity
        self.error_rate = error_rate
        self.growth_factor = growth_factor
        self.ratio = ratio
        self._meta_key = f"{key}:meta"

    async def _get_num_slices(self) -> int:
        """Read the current number of slices from the metadata hash."""
        val = await self.client.hget(self._meta_key, "num_slices")
        return int(val) if val else 0

    async def _set_num_slices(self, n: int) -> None:
        """Write the slice count to the metadata hash."""
        await self.client.hset(self._meta_key, "num_slices", str(n))

    def _slice_params(self, index: int) -> tuple[int, float]:
        """Calculate (capacity, error_rate) for the slice at the given index."""
        cap = self.initial_capacity * (self.growth_factor ** index)
        err = self.error_rate * (self.ratio ** index)
        return cap, err

    def _slice_filter(self, index: int) -> BloomFilter:
        """Construct a BloomFilter instance for the given slice index."""
        cap, err = self._slice_params(index)
        return BloomFilter(self.client, f"{self.key}:{index}", cap, err)

    async def _get_stored_ttl(self) -> Optional[int]:
        """
        Read the TTL value stored in the metadata hash.

        Returns:
            The stored TTL in seconds, or ``None`` if no TTL has been set.
        """
        val = await self.client.hget(self._meta_key, "ttl")
        return int(val) if val else None

    async def _apply_ttl_to_slice(self, bf: BloomFilter, ttl: Optional[int]) -> None:
        """
        Apply a TTL to a slice's bitmap and counter keys.

        Args:
            bf: The ``BloomFilter`` slice to expire.
            ttl: Seconds until expiry. If ``None``, no TTL is applied.
        """
        if ttl is not None and ttl > 0:
            pipe = self.client.pipeline()
            pipe.expire(bf.key, ttl)
            pipe.expire(bf._count_key, ttl)
            await pipe.execute()

    async def _active_slice(self) -> tuple[BloomFilter, int, Optional[int]]:
        """
        Return the current active (latest) slice, creating a new one
        if none exist or if the current slice has reached capacity.

        When a new slice is created, it automatically inherits the TTL
        that was previously set via ``set_ttl()``.  The inherited TTL
        is the **remaining** TTL of the metadata key (not the original
        value), so all keys expire at roughly the same wall-clock time.

        Returns:
            A 3-tuple of ``(bloom_filter, slice_index, pending_ttl)``.
            ``pending_ttl`` is the TTL in seconds to apply to the new
            slice **after** data has been written to it, or ``None`` if
            no TTL should be applied.
        """
        num = await self._get_num_slices()
        if num == 0:
            await self._set_num_slices(1)
            bf = self._slice_filter(0)
            # Compute pending TTL for the very first slice
            stored_ttl = await self._get_stored_ttl()
            pending_ttl: Optional[int] = None
            if stored_ttl is not None:
                remaining = await self.client.ttl(self._meta_key)
                pending_ttl = remaining if remaining > 0 else stored_ttl
            return bf, 0, pending_ttl
        idx = num - 1
        bf = self._slice_filter(idx)
        cnt = await bf.count
        if cnt >= bf.capacity:
            idx = num
            await self._set_num_slices(num + 1)
            bf = self._slice_filter(idx)
            # Compute pending TTL for the newly created slice.
            stored_ttl = await self._get_stored_ttl()
            pending_ttl = None
            if stored_ttl is not None:
                remaining = await self.client.ttl(self._meta_key)
                pending_ttl = remaining if remaining > 0 else stored_ttl
                logger.info(
                    "ScalableBloomFilter(%s) created slice %d — "
                    "inherited TTL of %d seconds from parent.",
                    self.key, idx, pending_ttl,
                )
            return bf, idx, pending_ttl
        return bf, idx, None

    async def add(self, item: str) -> None:
        """Add an item, auto-creating a new slice if the current one is full."""
        bf, _, pending_ttl = await self._active_slice()
        await bf.add(item)
        # Apply TTL *after* writing so that EXPIRE targets an existing key.
        if pending_ttl is not None:
            await self._apply_ttl_to_slice(bf, pending_ttl)

    async def exists(self, item: str) -> bool:
        """Check all slices — returns True if any slice reports membership."""
        num = await self._get_num_slices()
        for i in range(num):
            if await self._slice_filter(i).exists(item):
                return True
        return False

    async def add_many(self, items: Iterable[str]) -> None:
        """Add multiple items, auto-scaling as needed."""
        for item in items:
            await self.add(item)

    async def exists_many(self, items: Iterable[str]) -> Dict[str, bool]:
        """Check multiple items across all slices in one pass per slice."""
        items_list = list(items)
        results: Dict[str, bool] = {item: False for item in items_list}
        num = await self._get_num_slices()
        for i in range(num):
            remaining = [item for item in items_list if not results[item]]
            if not remaining:
                break
            for item, found in (await self._slice_filter(i).exists_many(remaining)).items():
                if found:
                    results[item] = True
        return results

    @property
    async def count(self) -> int:
        """Total items across all slices."""
        num = await self._get_num_slices()
        total = 0
        for i in range(num):
            total += await self._slice_filter(i).count
        return total

    async def info(self) -> Dict[str, Any]:
        """
        Return overall filter configuration and per-slice details.

        Returns:
            A dict with the following keys:

            - **key** (``str``): Base Redis key.
            - **initial_capacity** (``int``): Capacity of the first slice.
            - **error_rate** (``float``): Overall target FP probability.
            - **growth_factor** (``int``): Capacity multiplier per slice.
            - **ratio** (``float``): Error-rate tightening ratio per slice.
            - **num_slices** (``int``): Number of slices currently allocated.
            - **total_count** (``int``): Sum of items across all slices.
            - **slices** (``list[dict]``): Per-slice ``info()`` output.
        """
        num = await self._get_num_slices()
        slices_info: List[Dict[str, Any]] = []
        total_count = 0
        for i in range(num):
            si = await self._slice_filter(i).info()
            slices_info.append(si)
            total_count += si["count"]
        return {
            "key": self.key,
            "initial_capacity": self.initial_capacity,
            "error_rate": self.error_rate,
            "growth_factor": self.growth_factor,
            "ratio": self.ratio,
            "num_slices": num,
            "total_count": total_count,
            "slices": slices_info,
        }

    async def clear(self) -> None:
        """Delete all slice keys, counters, and metadata from Redis."""
        num = await self._get_num_slices()
        pipe = self.client.pipeline()
        for i in range(num):
            bf = self._slice_filter(i)
            pipe.delete(bf.key, bf._count_key)
        pipe.delete(self._meta_key)
        await pipe.execute()

    async def set_ttl(self, seconds: int) -> None:
        """
        Set a TTL on all existing slice keys, counters, and metadata.

        The TTL value is also persisted in the metadata hash so that
        **future slices** created by auto-scaling will automatically
        inherit the remaining TTL at creation time.  This ensures all
        keys — including those that don't exist yet — expire at
        roughly the same wall-clock time.

        Args:
            seconds: Number of seconds until all keys expire.
        """
        num = await self._get_num_slices()
        pipe = self.client.pipeline()
        # Persist the TTL value for future slice inheritance
        pipe.hset(self._meta_key, "ttl", str(seconds))
        for i in range(num):
            bf = self._slice_filter(i)
            pipe.expire(bf.key, seconds)
            pipe.expire(bf._count_key, seconds)
        pipe.expire(self._meta_key, seconds)
        await pipe.execute()

    async def get_ttl(self) -> int:
        """Return remaining TTL of the metadata key."""
        return await self.client.ttl(self._meta_key)

    def __repr__(self) -> str:
        return (
            f"ScalableBloomFilter(key={self.key!r}, "
            f"initial_capacity={self.initial_capacity}, "
            f"error_rate={self.error_rate}, "
            f"growth_factor={self.growth_factor}, ratio={self.ratio})"
        )
