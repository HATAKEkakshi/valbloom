"""
Comprehensive tests for ValBloom bloom filter library.
"""

import pytest
import pytest_asyncio

from valbloom import (
    BloomFilter,
    CountingBloomFilter,
    ScalableBloomFilter,
    IncompatibleFilterError,
)


class TestBloomFilter:

    @pytest.mark.asyncio
    async def test_add_and_exists(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        await bf.add("hello")
        assert await bf.exists("hello") is True

    @pytest.mark.asyncio
    async def test_not_exists(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        assert await bf.exists("missing") is False

    @pytest.mark.asyncio
    async def test_multiple_items(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        items = ["alpha", "bravo", "charlie"]
        for item in items:
            await bf.add(item)
        for item in items:
            assert await bf.exists(item) is True
        assert await bf.exists("delta") is False

    @pytest.mark.asyncio
    async def test_add_many(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        items = ["one", "two", "three"]
        await bf.add_many(items)
        for item in items:
            assert await bf.exists(item) is True

    @pytest.mark.asyncio
    async def test_exists_many(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        await bf.add_many(["a", "b"])
        results = await bf.exists_many(["a", "b", "c"])
        assert results["a"] is True
        assert results["b"] is True
        assert results["c"] is False

    @pytest.mark.asyncio
    async def test_count(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        assert await bf.count == 0
        await bf.add("x")
        assert await bf.count == 1
        await bf.add_many(["y", "z"])
        assert await bf.count == 3

    @pytest.mark.asyncio
    async def test_clear(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        await bf.add("item")
        await bf.clear()
        assert await bf.count == 0
        assert await bf.exists("item") is False

    @pytest.mark.asyncio
    async def test_info(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000, error_rate=0.01)
        await bf.add("data")
        info = await bf.info()
        assert info["key"] == "bf:test"
        assert info["capacity"] == 1000
        assert info["error_rate"] == 0.01
        assert info["count"] == 1
        assert info["size_bits"] == bf.m
        assert info["num_hashes"] == bf.k
        assert 0 < info["fill_ratio"] < 1

    @pytest.mark.asyncio
    async def test_ttl(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        await bf.add("item")
        await bf.set_ttl(300)
        ttl = await bf.get_ttl()
        assert 0 < ttl <= 300

    @pytest.mark.asyncio
    async def test_union(self, redis):
        bf1 = BloomFilter(redis, "bf:a", capacity=1000)
        bf2 = BloomFilter(redis, "bf:b", capacity=1000)
        # Add a shared item so both bitmaps overlap
        await bf1.add("shared")
        await bf2.add("shared")
        merged = await bf1.union(bf2, dest_key="bf:merged")
        assert await merged.exists("shared") is True
        # Verify non-members remain absent
        assert await merged.exists("never-added") is False

    @pytest.mark.asyncio
    async def test_union_returns_compatible_filter(self, redis):
        bf1 = BloomFilter(redis, "bf:a", capacity=1000)
        bf2 = BloomFilter(redis, "bf:b", capacity=1000)
        await bf1.add("x")
        await bf2.add("y")
        merged = await bf1.union(bf2, dest_key="bf:merged")
        assert merged.key == "bf:merged"
        assert merged.m == bf1.m
        assert merged.k == bf1.k

    @pytest.mark.asyncio
    async def test_intersection(self, redis):
        bf1 = BloomFilter(redis, "bf:a", capacity=1000)
        bf2 = BloomFilter(redis, "bf:b", capacity=1000)
        await bf1.add("shared")
        await bf1.add("only-a")
        await bf2.add("shared")
        await bf2.add("only-b")
        result = await bf1.intersection(bf2, dest_key="bf:inter")
        assert await result.exists("shared") is True
        # Items only in one filter should not survive intersection
        assert await result.exists("only-a") is False
        assert await result.exists("only-b") is False

    @pytest.mark.asyncio
    async def test_incompatible_union_raises(self, redis):
        bf1 = BloomFilter(redis, "bf:a", capacity=1000)
        bf2 = BloomFilter(redis, "bf:b", capacity=5000)  # different m/k
        with pytest.raises(IncompatibleFilterError):
            await bf1.union(bf2)

    def test_invalid_capacity(self, redis):
        with pytest.raises(ValueError):
            BloomFilter(redis, "bf:bad", capacity=0)
        with pytest.raises(ValueError):
            BloomFilter(redis, "bf:bad", capacity=-10)

    def test_invalid_error_rate(self, redis):
        with pytest.raises(ValueError):
            BloomFilter(redis, "bf:bad", capacity=1000, error_rate=0)
        with pytest.raises(ValueError):
            BloomFilter(redis, "bf:bad", capacity=1000, error_rate=1.0)
        with pytest.raises(ValueError):
            BloomFilter(redis, "bf:bad", capacity=1000, error_rate=1.5)

    def test_repr(self, redis):
        bf = BloomFilter(redis, "bf:test", capacity=1000)
        r = repr(bf)
        assert "BloomFilter" in r
        assert "bf:test" in r
        assert "1000" in r

    @pytest.mark.asyncio
    async def test_false_positive_rate_within_bounds(self, redis):
        """Add capacity items and check FP rate stays near error_rate."""
        cap = 500
        err = 0.05
        bf = BloomFilter(redis, "bf:fp", capacity=cap, error_rate=err)

        # Add items 0..cap-1
        await bf.add_many([str(i) for i in range(cap)])

        # Check 1000 items that were NOT added
        fp_count = 0
        test_range = 1000
        for i in range(cap, cap + test_range):
            if await bf.exists(str(i)):
                fp_count += 1

        fp_rate = fp_count / test_range
        # Allow generous margin — FP rate should be under 3x the target
        assert fp_rate < err * 3, f"FP rate {fp_rate:.4f} exceeds 3x target {err}"


class TestCountingBloomFilter:

    @pytest.mark.asyncio
    async def test_add_and_exists(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        await cbf.add("hello")
        assert await cbf.exists("hello") is True

    @pytest.mark.asyncio
    async def test_not_exists(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        assert await cbf.exists("nope") is False

    @pytest.mark.asyncio
    async def test_remove(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        await cbf.add("temp")
        assert await cbf.exists("temp") is True
        await cbf.remove("temp")
        assert await cbf.exists("temp") is False

    @pytest.mark.asyncio
    async def test_remove_does_not_affect_others(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        await cbf.add("keep")
        await cbf.add("remove-me")
        await cbf.remove("remove-me")
        # "keep" should still be present
        assert await cbf.exists("keep") is True

    @pytest.mark.asyncio
    async def test_double_add_then_single_remove(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        await cbf.add("double")
        await cbf.add("double")
        await cbf.remove("double")
        # Counters should still be > 0 after one removal
        assert await cbf.exists("double") is True

    @pytest.mark.asyncio
    async def test_add_many_and_exists_many(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        await cbf.add_many(["a", "b", "c"])
        results = await cbf.exists_many(["a", "b", "c", "d"])
        assert results["a"] is True
        assert results["d"] is False

    @pytest.mark.asyncio
    async def test_count(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        await cbf.add("x")
        await cbf.add("y")
        assert await cbf.count == 2
        await cbf.remove("x")
        assert await cbf.count == 1

    @pytest.mark.asyncio
    async def test_clear(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        await cbf.add("item")
        await cbf.clear()
        assert await cbf.count == 0
        assert await cbf.exists("item") is False

    @pytest.mark.asyncio
    async def test_info(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        await cbf.add("x")
        info = await cbf.info()
        assert info["key"] == "cbf:test"
        assert info["count"] == 1

    def test_repr(self, redis):
        cbf = CountingBloomFilter(redis, "cbf:test", capacity=1000)
        assert "CountingBloomFilter" in repr(cbf)

    def test_invalid_params(self, redis):
        with pytest.raises(ValueError):
            CountingBloomFilter(redis, "cbf:bad", capacity=-1)

    @pytest.mark.asyncio
    async def test_duplicate_add_emits_warning(self, redis, caplog):
        """Adding the same item twice should emit a WARNING log."""
        import logging
        cbf = CountingBloomFilter(redis, "cbf:dup", capacity=1000)
        with caplog.at_level(logging.WARNING, logger="valbloom.bloom"):
            await cbf.add("session-xyz")
            assert len(caplog.records) == 0  # first add — no warning
            await cbf.add("session-xyz")
            assert any("already exist" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_duplicate_add_remove_semantics(self, redis):
        """add() twice → remove() once → exists() should still be True."""
        cbf = CountingBloomFilter(redis, "cbf:sem", capacity=1000)
        await cbf.add("item")
        await cbf.add("item")
        await cbf.remove("item")
        assert await cbf.exists("item") is True
        await cbf.remove("item")
        assert await cbf.exists("item") is False


class TestScalableBloomFilter:

    @pytest.mark.asyncio
    async def test_add_and_exists(self, redis):
        sbf = ScalableBloomFilter(redis, "sbf:test", initial_capacity=100)
        await sbf.add("hello")
        assert await sbf.exists("hello") is True

    @pytest.mark.asyncio
    async def test_not_exists(self, redis):
        sbf = ScalableBloomFilter(redis, "sbf:test", initial_capacity=100)
        assert await sbf.exists("nope") is False

    @pytest.mark.asyncio
    async def test_auto_scales(self, redis):
        """Adding more items than initial capacity creates new slices."""
        cap = 50
        sbf = ScalableBloomFilter(redis, "sbf:scale", initial_capacity=cap)
        items = [f"item-{i}" for i in range(cap + 20)]
        await sbf.add_many(items)

        info = await sbf.info()
        assert info["num_slices"] > 1, "Should have scaled beyond 1 slice"

        # All items should still be findable
        for item in items:
            assert await sbf.exists(item) is True, f"{item} not found after scaling"

    @pytest.mark.asyncio
    async def test_count_spans_slices(self, redis):
        cap = 50
        sbf = ScalableBloomFilter(redis, "sbf:count", initial_capacity=cap)
        n = cap + 30
        await sbf.add_many([f"i-{i}" for i in range(n)])
        assert await sbf.count == n

    @pytest.mark.asyncio
    async def test_exists_many(self, redis):
        sbf = ScalableBloomFilter(redis, "sbf:test", initial_capacity=100)
        await sbf.add_many(["a", "b"])
        results = await sbf.exists_many(["a", "b", "c"])
        assert results["a"] is True
        assert results["c"] is False

    @pytest.mark.asyncio
    async def test_clear(self, redis):
        sbf = ScalableBloomFilter(redis, "sbf:test", initial_capacity=50)
        await sbf.add_many([f"x-{i}" for i in range(60)])
        await sbf.clear()
        assert await sbf.count == 0
        assert await sbf.exists("x-0") is False

    @pytest.mark.asyncio
    async def test_info(self, redis):
        sbf = ScalableBloomFilter(redis, "sbf:test", initial_capacity=100)
        await sbf.add("item")
        info = await sbf.info()
        assert info["key"] == "sbf:test"
        assert info["num_slices"] == 1
        assert info["total_count"] == 1
        assert len(info["slices"]) == 1

    def test_repr(self, redis):
        sbf = ScalableBloomFilter(redis, "sbf:test", initial_capacity=100)
        assert "ScalableBloomFilter" in repr(sbf)

    def test_invalid_params(self, redis):
        with pytest.raises(ValueError):
            ScalableBloomFilter(redis, "sbf:bad", initial_capacity=0)
        with pytest.raises(ValueError):
            ScalableBloomFilter(redis, "sbf:bad", initial_capacity=100, growth_factor=0)
        with pytest.raises(ValueError):
            ScalableBloomFilter(redis, "sbf:bad", initial_capacity=100, ratio=1.5)

    @pytest.mark.asyncio
    async def test_ttl_inherited_on_scaling(self, redis):
        """New slices created by auto-scaling should inherit the parent TTL."""
        cap = 50
        sbf = ScalableBloomFilter(redis, "sbf:ttl", initial_capacity=cap)
        # Add items to fill first slice
        await sbf.add_many([f"i-{i}" for i in range(cap)])
        # Set TTL before scaling triggers
        await sbf.set_ttl(600)
        # This add should trigger a new slice
        await sbf.add("overflow")
        info = await sbf.info()
        assert info["num_slices"] > 1
        # Check that the new slice key has a TTL set
        new_bf = sbf._slice_filter(info["num_slices"] - 1)
        ttl = await redis.ttl(new_bf.key)
        assert ttl > 0, "New slice should have inherited a TTL"


class TestIncompatibleFilterError:

    @pytest.mark.asyncio
    async def test_error_includes_capacity_and_error_rate(self, redis):
        """Error message should include capacity and error_rate for debugging."""
        bf1 = BloomFilter(redis, "bf:e1", capacity=1000, error_rate=0.001)
        bf2 = BloomFilter(redis, "bf:e2", capacity=5000, error_rate=0.01)
        with pytest.raises(IncompatibleFilterError, match=r"capacity=.*error_rate="):
            await bf1.union(bf2)
