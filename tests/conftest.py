"""
PyTest configuration and shared fixtures for the distributed cache tests.

This file provides common test utilities and fixtures that can be used
across all test modules.
"""

import pytest
import sys
import os
import time
import threading
from typing import Generator, List, Tuple

# Add src directory to Python path for all tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import after path setup
from consistent_hash import ConsistentHashRing
from cache_node import CacheStorage
from client import CacheClient


@pytest.fixture
def sample_nodes() -> List[Tuple[str, int]]:
    """Standard set of nodes for testing."""
    return [
        ("test_node1", 8001),
        ("test_node2", 8002), 
        ("test_node3", 8003)
    ]


@pytest.fixture
def hash_ring() -> ConsistentHashRing:
    """Create a hash ring with test nodes."""
    return ConsistentHashRing(["node1", "node2", "node3"])


@pytest.fixture
def cache_storage() -> CacheStorage:
    """Create a fresh cache storage instance."""
    return CacheStorage()


@pytest.fixture
def test_client(sample_nodes) -> CacheClient:
    """Create a test client (note: requires running servers for integration tests)."""
    return CacheClient(sample_nodes, timeout=1.0)


@pytest.fixture
def sample_data() -> dict:
    """Sample data for testing."""
    return {
        "simple_string": "hello world",
        "number": 42,
        "boolean": True,
        "null_value": None,
        "list": [1, 2, 3, "four", 5.0],
        "dict": {
            "nested": "value",
            "count": 100,
            "settings": {
                "enabled": True,
                "threshold": 0.95
            }
        }
    }


@pytest.fixture
def performance_keys() -> List[str]:
    """Generate keys for performance testing."""
    return [f"perf_key_{i:06d}" for i in range(1000)]


# Pytest markers for organizing tests
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "unit: Unit tests (fast, no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (require running servers)"
    )
    config.addinivalue_line(
        "markers", "performance: Performance benchmarks"
    )
    config.addinivalue_line(
        "markers", "slow: Slow running tests"
    )


# Custom test utilities
class TestUtils:
    """Utility functions for tests."""
    
    @staticmethod
    def wait_for_condition(condition_func, timeout=5.0, interval=0.1):
        """Wait for a condition to become true."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if condition_func():
                return True
            time.sleep(interval)
        return False
    
    @staticmethod
    def measure_time(func, *args, **kwargs):
        """Measure execution time of a function."""
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        return result, duration
    
    @staticmethod
    def generate_test_keys(prefix="test", count=100):
        """Generate test keys with a given prefix."""
        return [f"{prefix}_key_{i:04d}" for i in range(count)]
    
    @staticmethod
    def assert_distribution_balance(distribution, tolerance=0.4):
        """Assert that load distribution is reasonably balanced."""
        if not distribution:
            return
        
        values = list(distribution.values())
        avg = sum(values) / len(values)
        
        for node, value in distribution.items():
            ratio = value / avg
            assert (1 - tolerance) < ratio < (1 + tolerance), \
                f"Node {node} has {value:.2f}%, expected ~{avg:.2f}% (±{tolerance*100}%)"


@pytest.fixture
def test_utils() -> TestUtils:
    """Provide test utilities."""
    return TestUtils()


# Performance testing helpers
@pytest.fixture
def benchmark_decorator():
    """Decorator for benchmark tests."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            # Print benchmark results
            func_name = func.__name__
            print(f"\n📊 Benchmark: {func_name}")
            print(f"   Duration: {duration:.4f} seconds")
            
            return result
        return wrapper
    return decorator


# Session-level fixtures for expensive setup
@pytest.fixture(scope="session")
def large_dataset():
    """Generate a large dataset for performance testing."""
    print("\n🔧 Generating large test dataset...")
    
    dataset = {}
    for i in range(10000):
        key = f"large_dataset_key_{i:06d}"
        value = {
            "id": i,
            "name": f"item_{i}",
            "data": f"large_data_value_{i}" * 10,  # Make it somewhat large
            "metadata": {
                "created": f"2024-01-{(i % 28) + 1:02d}",
                "category": f"category_{i % 10}",
                "priority": i % 5
            }
        }
        dataset[key] = value
    
    print(f"✅ Generated {len(dataset)} items for testing")
    return dataset


# Cleanup fixtures
@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Cleanup after each test."""
    yield
    # Any cleanup code would go here
    pass


# Skip markers for conditional tests
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add skip markers."""
    for item in items:
        # Skip integration tests if no servers are running
        if "integration" in item.keywords:
            item.add_marker(pytest.mark.skipif(
                not _check_test_servers_available(),
                reason="Integration tests require running cache servers"
            ))


def _check_test_servers_available() -> bool:
    """Check if test servers are available for integration tests."""
    try:
        import requests
        # Try to connect to a test server
        response = requests.get("http://localhost:9001/health", timeout=1)
        return response.status_code == 200
    except:
        return False


# Useful constants for tests
TEST_PORTS = [9001, 9002, 9003]
TEST_NODES = [f"test_node_{i}" for i in range(1, 4)]
PERFORMANCE_THRESHOLDS = {
    "hash_lookup_ops_per_sec": 10000,
    "cache_ops_per_sec": 100,
    "max_latency_ms": 10.0
}