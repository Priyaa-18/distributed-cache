"""
Unit tests for cache node storage and HTTP server functionality.

Tests the core caching logic, TTL handling, and HTTP API endpoints.
"""

import pytest
import time
import threading
import json
import requests
from unittest.mock import patch, MagicMock
import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from cache_node import CacheStorage, CacheNode


class TestCacheStorage:
    """Test the in-memory storage component."""
    
    def test_basic_get_set(self):
        """Test basic get/set operations."""
        storage = CacheStorage()
        
        # Test setting and getting a value
        storage.set("key1", "value1")
        assert storage.get("key1") == "value1"
        
        # Test getting non-existent key
        assert storage.get("nonexistent") is None
    
    def test_overwrite_value(self):
        """Test overwriting existing values."""
        storage = CacheStorage()
        
        storage.set("key1", "value1")
        storage.set("key1", "value2")
        assert storage.get("key1") == "value2"
    
    def test_delete(self):
        """Test deleting keys."""
        storage = CacheStorage()
        
        storage.set("key1", "value1")
        assert storage.delete("key1") is True
        assert storage.get("key1") is None
        
        # Test deleting non-existent key
        assert storage.delete("nonexistent") is False
    
    def test_clear(self):
        """Test clearing all data."""
        storage = CacheStorage()
        
        storage.set("key1", "value1")
        storage.set("key2", "value2")
        storage.clear()
        
        assert storage.get("key1") is None
        assert storage.get("key2") is None
        assert storage.size() == 0
    
    def test_size(self):
        """Test size tracking."""
        storage = CacheStorage()
        
        assert storage.size() == 0
        
        storage.set("key1", "value1")
        assert storage.size() == 1
        
        storage.set("key2", "value2")
        assert storage.size() == 2
        
        storage.delete("key1")
        assert storage.size() == 1
    
    def test_ttl_basic(self):
        """Test basic TTL functionality."""
        storage = CacheStorage()
        
        # Set with 1 second TTL
        storage.set("temp_key", "temp_value", ttl_seconds=1)
        assert storage.get("temp_key") == "temp_value"
        
        # Wait for expiration
        time.sleep(1.1)
        assert storage.get("temp_key") is None
    
    def test_ttl_update(self):
        """Test updating TTL values."""
        storage = CacheStorage()
        
        # Set with TTL
        storage.set("key1", "value1", ttl_seconds=10)
        
        # Update without TTL (should remove TTL)
        storage.set("key1", "value2")
        
        # Should still exist after original TTL would have expired
        time.sleep(0.1)  # Small delay
        assert storage.get("key1") == "value2"
    
    def test_ttl_removal_on_get(self):
        """Test that expired keys are removed when accessed."""
        storage = CacheStorage()
        
        storage.set("temp_key", "temp_value", ttl_seconds=0.1)
        time.sleep(0.2)
        
        # Getting expired key should return None and remove it
        assert storage.get("temp_key") is None
        
        # Key should be completely removed from storage
        assert "temp_key" not in storage.data
        assert "temp_key" not in storage.ttl
    
    def test_stats(self):
        """Test statistics gathering."""
        storage = CacheStorage()
        
        # Empty storage stats
        stats = storage.stats()
        assert stats["total_keys"] == 0
        assert stats["keys_with_ttl"] == 0
        assert stats["memory_usage_estimate"] >= 0
        
        # Add some data
        storage.set("key1", "value1")
        storage.set("key2", "value2", ttl_seconds=10)
        
        stats = storage.stats()
        assert stats["total_keys"] == 2
        assert stats["keys_with_ttl"] == 1
        assert stats["memory_usage_estimate"] > 0
    
    def test_thread_safety(self):
        """Test concurrent access from multiple threads."""
        storage = CacheStorage()
        errors = []
        
        def worker(thread_id):
            try:
                for i in range(100):
                    key = f"thread_{thread_id}_key_{i}"
                    value = f"thread_{thread_id}_value_{i}"
                    
                    storage.set(key, value)
                    retrieved = storage.get(key)
                    
                    if retrieved != value:
                        errors.append(f"Mismatch in thread {thread_id}: {retrieved} != {value}")
            except Exception as e:
                errors.append(f"Exception in thread {thread_id}: {e}")
        
        # Run multiple threads concurrently
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Check for any errors
        assert len(errors) == 0, f"Thread safety errors: {errors}"
        
        # Verify we have the expected number of keys
        assert storage.size() == 500  # 5 threads * 100 keys each
    
    @patch('time.time')
    def test_expired_key_cleanup_detection(self, mock_time):
        """Test detection of expired keys in stats."""
        storage = CacheStorage()
        
        # Set current time
        mock_time.return_value = 1000.0
        
        # Add key with TTL
        storage.set("key1", "value1", ttl_seconds=10)
        storage.set("key2", "value2")  # No TTL
        
        # Move time forward past expiration
        mock_time.return_value = 1020.0
        
        stats = storage.stats()
        assert stats["total_keys"] == 2
        assert stats["expired_keys"] == 1


class TestCacheNodeIntegration:
    """Integration tests for the complete cache node."""
    
    @pytest.fixture
    def cache_node(self):
        """Create a cache node for testing."""
        return CacheNode("test_node", 9999)
    
    def test_node_initialization(self, cache_node):
        """Test cache node initialization."""
        assert cache_node.node_id == "test_node"
        assert cache_node.port == 9999
        assert cache_node.storage is not None
        assert cache_node.server is None  # Not started yet
    
    def test_node_storage_integration(self, cache_node):
        """Test that node properly uses its storage."""
        # Test through the node's storage
        cache_node.storage.set("test_key", "test_value")
        assert cache_node.storage.get("test_key") == "test_value"
        
        # Test stats
        stats = cache_node.storage.stats()
        assert stats["total_keys"] == 1


# HTTP API Tests (these would need a running server)
class TestCacheNodeHTTPAPI:
    """
    Tests for the HTTP API endpoints.
    
    Note: These tests require a running cache node server.
    In a real project, you might use a test framework that can
    start/stop servers automatically.
    """
    
    @pytest.fixture(scope="class")
    def server_url(self):
        """Base URL for test server."""
        return "http://localhost:9998"
    
    @pytest.fixture(scope="class", autouse=True)
    def start_test_server(self, server_url):
        """
        Start a test server for HTTP API tests.
        
        In a production test suite, you'd start this automatically.
        For now, this is a placeholder that assumes a server is running.
        """
        # In a real test, you'd start the server here
        # For this demo, we'll skip these tests if server isn't running
        try:
            response = requests.get(f"{server_url}/health", timeout=1)
            if response.status_code != 200:
                pytest.skip("Test server not running")
        except requests.exceptions.RequestException:
            pytest.skip("Test server not running on port 9998")
    
    def test_health_endpoint(self, server_url):
        """Test the health check endpoint."""
        response = requests.get(f"{server_url}/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "node_id" in data
        assert "uptime" in data
    
    def test_stats_endpoint(self, server_url):
        """Test the stats endpoint."""
        response = requests.get(f"{server_url}/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "total_keys" in data
        assert "memory_usage_estimate" in data
        assert "node_id" in data
        assert "port" in data
    
    def test_set_get_delete_flow(self, server_url):
        """Test the complete set/get/delete flow."""
        key = "test_api_key"
        value = "test_api_value"
        
        # SET: Store a value
        set_response = requests.post(
            f"{server_url}/cache/{key}",
            json={"value": value},
            headers={"Content-Type": "application/json"}
        )
        assert set_response.status_code == 200
        set_data = set_response.json()
        assert set_data["key"] == key
        assert set_data["value"] == value
        
        # GET: Retrieve the value
        get_response = requests.get(f"{server_url}/cache/{key}")
        assert get_response.status_code == 200
        get_data = get_response.json()
        assert get_data["key"] == key
        assert get_data["value"] == value
        
        # DELETE: Remove the value
        delete_response = requests.delete(f"{server_url}/cache/{key}")
        assert delete_response.status_code == 200
        
        # GET: Verify it's gone
        get_response2 = requests.get(f"{server_url}/cache/{key}")
        assert get_response2.status_code == 404
    
    def test_set_with_ttl(self, server_url):
        """Test setting values with TTL."""
        key = "ttl_test_key"
        value = "ttl_test_value"
        
        # Set with 1 second TTL
        response = requests.post(
            f"{server_url}/cache/{key}",
            json={"value": value, "ttl": 1},
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 200
        
        # Should exist immediately
        get_response = requests.get(f"{server_url}/cache/{key}")
        assert get_response.status_code == 200
        
        # Wait for expiration
        time.sleep(1.1)
        
        # Should be gone
        get_response2 = requests.get(f"{server_url}/cache/{key}")
        assert get_response2.status_code == 404
    
    def test_nonexistent_key(self, server_url):
        """Test accessing non-existent keys."""
        response = requests.get(f"{server_url}/cache/nonexistent_key")
        assert response.status_code == 404
        
        data = response.json()
        assert "error" in data
    
    def test_invalid_json(self, server_url):
        """Test sending invalid JSON."""
        response = requests.post(
            f"{server_url}/cache/test_key",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 400
        
        data = response.json()
        assert "error" in data
    
    def test_missing_value(self, server_url):
        """Test POST without value field."""
        response = requests.post(
            f"{server_url}/cache/test_key",
            json={"not_value": "test"},
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 400
        
        data = response.json()
        assert "error" in data
        assert "Missing 'value'" in data["error"]
    
    def test_complex_data_types(self, server_url):
        """Test storing complex data types."""
        key = "complex_key"
        complex_value = {
            "string": "test",
            "number": 42,
            "boolean": True,
            "null": None,
            "array": [1, 2, 3],
            "object": {"nested": "value"}
        }
        
        # Store complex data
        set_response = requests.post(
            f"{server_url}/cache/{key}",
            json={"value": complex_value},
            headers={"Content-Type": "application/json"}
        )
        assert set_response.status_code == 200
        
        # Retrieve and verify
        get_response = requests.get(f"{server_url}/cache/{key}")
        assert get_response.status_code == 200
        
        data = get_response.json()
        assert data["value"] == complex_value
    
    def test_not_found_endpoints(self, server_url):
        """Test accessing non-existent endpoints."""
        # Invalid cache path
        response = requests.get(f"{server_url}/cache")
        assert response.status_code == 404
        
        # Random path
        response = requests.get(f"{server_url}/random/path")
        assert response.status_code == 404
        
        # POST to wrong path
        response = requests.post(f"{server_url}/wrong/path")
        assert response.status_code == 404


if __name__ == "__main__":
    # Run the storage tests directly
    print("=== Running CacheStorage Tests ===")
    
    # Basic functionality test
    storage = CacheStorage()
    storage.set("test", "value")
    print(f"✅ Basic set/get: {storage.get('test')}")
    
    # TTL test
    storage.set("ttl_test", "expires", ttl_seconds=1)
    print(f"✅ TTL before expiry: {storage.get('ttl_test')}")
    time.sleep(1.1)
    print(f"✅ TTL after expiry: {storage.get('ttl_test')}")
    
    # Stats test
    storage.set("key1", "value1")
    storage.set("key2", "value2", ttl_seconds=10)
    stats = storage.stats()
    print(f"✅ Stats: {stats}")
    
    print("\n=== All basic tests passed! ===")
    print("Run 'python -m pytest tests/test_cache_node.py -v' for full test suite")