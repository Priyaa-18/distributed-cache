"""
Integration tests for the complete distributed cache system.

These tests verify that all components work together correctly:
- Consistent hashing + Cache nodes + Smart client
- End-to-end workflows
- Fault tolerance scenarios
- Performance characteristics
"""

import pytest
import time
import threading
import subprocess
import sys
import os
import requests
from typing import List, Dict, Any

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from client import CacheClient
from cache_node import CacheNode
from consistent_hash import ConsistentHashRing


class TestClusterManager:
    """Helper class to manage a test cluster."""
    
    def __init__(self, nodes: List[tuple]):
        self.nodes = nodes
        self.processes = []
        self.client = None
    
    def start_cluster(self):
        """Start all cache nodes."""
        for node_id, port in self.nodes:
            # Start cache node in subprocess
            process = subprocess.Popen(
                [sys.executable, "-c", f"""
import sys
sys.path.append('src')
from cache_node import CacheNode
node = CacheNode('{node_id}', {port})
node.start()
"""],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.processes.append(process)
        
        # Wait for nodes to start
        time.sleep(2)
        
        # Create client
        self.client = CacheClient(self.nodes, timeout=2.0)
        
        # Wait for all nodes to be healthy
        for _ in range(10):
            health = self.client.get_cluster_health()
            if health['cluster_healthy']:
                break
            time.sleep(0.5)
        else:
            raise Exception("Cluster failed to start")
    
    def stop_cluster(self):
        """Stop all cache nodes."""
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
        self.processes.clear()


@pytest.fixture(scope="module")
def test_cluster():
    """Create a test cluster for integration tests."""
    nodes = [
        ("test_node1", 9001),
        ("test_node2", 9002), 
        ("test_node3", 9003)
    ]
    
    cluster = TestClusterManager(nodes)
    
    try:
        cluster.start_cluster()
        yield cluster
    finally:
        cluster.stop_cluster()


class TestBasicIntegration:
    """Test basic end-to-end functionality."""
    
    def test_single_operation_flow(self, test_cluster):
        """Test a complete set/get/delete flow."""
        client = test_cluster.client
        
        key = "integration_test_key"
        value = "integration_test_value"
        
        # Set
        success = client.set(key, value)
        assert success is True
        
        # Get
        retrieved = client.get(key)
        assert retrieved == value
        
        # Delete
        deleted = client.delete(key)
        assert deleted is True
        
        # Verify deleted
        retrieved_after_delete = client.get(key)
        assert retrieved_after_delete is None
    
    def test_multiple_keys_distribution(self, test_cluster):
        """Test that multiple keys are distributed across nodes."""
        client = test_cluster.client
        
        test_data = {
            f"key_{i:03d}": f"value_{i:03d}" 
            for i in range(50)
        }
        
        # Store all data
        for key, value in test_data.items():
            success = client.set(key, value)
            assert success is True
        
        # Verify all data can be retrieved
        for key, expected_value in test_data.items():
            retrieved = client.get(key)
            assert retrieved == expected_value
        
        # Check distribution across nodes
        node_assignments = {}
        for key in test_data.keys():
            node = client.hash_ring.get_node(key)
            node_assignments[node] = node_assignments.get(node, 0) + 1
        
        # Should use multiple nodes
        assert len(node_assignments) > 1
        print(f"Key distribution: {node_assignments}")
    
    def test_complex_data_types(self, test_cluster):
        """Test storing and retrieving complex data types."""
        client = test_cluster.client
        
        complex_data = {
            "user_profile": {
                "id": 12345,
                "name": "Alice Johnson",
                "email": "alice@example.com",
                "preferences": {
                    "theme": "dark",
                    "notifications": True,
                    "languages": ["en", "es", "fr"]
                },
                "metadata": {
                    "created_at": "2024-01-01T00:00:00Z",
                    "last_login": "2024-01-15T10:30:00Z",
                    "login_count": 42
                }
            },
            "shopping_cart": [
                {"id": 1, "name": "Laptop", "price": 999.99, "quantity": 1},
                {"id": 2, "name": "Mouse", "price": 29.99, "quantity": 2}
            ],
            "session_data": {
                "session_id": "sess_abc123",
                "csrf_token": "token_xyz789",
                "permissions": ["read", "write", "admin"]
            }
        }
        
        # Store all complex data
        for key, value in complex_data.items():
            success = client.set(key, value)
            assert success is True
        
        # Retrieve and verify
        for key, expected_value in complex_data.items():
            retrieved = client.get(key)
            assert retrieved == expected_value
    
    def test_ttl_functionality(self, test_cluster):
        """Test TTL (time to live) functionality."""
        client = test_cluster.client
        
        key = "ttl_test_key"
        value = "ttl_test_value"
        
        # Set with 2 second TTL
        success = client.set(key, value, ttl=2)
        assert success is True
        
        # Should exist immediately
        retrieved = client.get(key)
        assert retrieved == value
        
        # Should still exist after 1 second
        time.sleep(1)
        retrieved = client.get(key)
        assert retrieved == value
        
        # Should be gone after 3 seconds total
        time.sleep(2.5)
        retrieved = client.get(key)
        assert retrieved is None


class TestConsistentHashingIntegration:
    """Test consistent hashing behavior in real system."""
    
    def test_hash_ring_consistency(self, test_cluster):
        """Test that hash ring produces consistent results."""
        client = test_cluster.client
        
        test_keys = [f"consistency_test_{i}" for i in range(100)]
        
        # Get initial assignments
        initial_assignments = {}
        for key in test_keys:
            node = client.hash_ring.get_node(key)
            initial_assignments[key] = node
        
        # Check assignments multiple times
        for _ in range(5):
            for key in test_keys:
                current_node = client.hash_ring.get_node(key)
                assert current_node == initial_assignments[key], \
                    f"Key {key} changed nodes: {initial_assignments[key]} -> {current_node}"
    
    def test_load_distribution_real_data(self, test_cluster):
        """Test load distribution with real cache operations."""
        client = test_cluster.client
        
        # Generate realistic cache keys
        user_keys = [f"user:{i:05d}" for i in range(100)]
        session_keys = [f"session:{i:08x}" for i in range(100)]
        cache_keys = [f"cache:popular_item_{i}" for i in range(100)]
        
        all_keys = user_keys + session_keys + cache_keys
        
        # Store all data
        for key in all_keys:
            success = client.set(key, f"data_for_{key}")
            assert success is True
        
        # Analyze distribution
        node_counts = {}
        for key in all_keys:
            node = client.hash_ring.get_node(key)
            node_counts[node] = node_counts.get(node, 0) + 1
        
        print(f"Real data distribution: {node_counts}")
        
        # Check reasonably even distribution (within 40% of average)
        total_keys = len(all_keys)
        expected_per_node = total_keys / len(test_cluster.nodes)
        
        for node, count in node_counts.items():
            ratio = count / expected_per_node
            assert 0.6 < ratio < 1.4, \
                f"Node {node} has {count} keys, expected ~{expected_per_node}"


class TestReplicationIntegration:
    """Test replication functionality in real system."""
    
    def test_basic_replication(self, test_cluster):
        """Test basic replication across multiple nodes."""
        client = test_cluster.client
        
        key = "replicated_key"
        value = "replicated_value"
        
        # Store with replication
        successful_writes = client.set_with_replication(key, value, replicas=3)
        assert successful_writes == 3  # Should write to all 3 nodes
        
        # Should be able to read from any replica
        retrieved = client.get_with_replication(key)
        assert retrieved == value
    
    def test_replication_fault_tolerance(self, test_cluster):
        """Test that replication provides fault tolerance."""
        client = test_cluster.client
        
        # Store data with replication
        test_data = {
            "critical_config": {"db_url": "postgres://...", "api_key": "secret"},
            "user_session": {"user_id": 12345, "permissions": ["read", "write"]},
            "cache_metadata": {"version": "1.0", "timestamp": "2024-01-01"}
        }
        
        for key, value in test_data.items():
            successful_writes = client.set_with_replication(key, value, replicas=3)
            assert successful_writes >= 2  # At least 2 replicas
        
        # Data should be retrievable even if we specify reading from replicas
        for key, expected_value in test_data.items():
            retrieved = client.get_with_replication(key, read_quorum=1)
            assert retrieved == expected_value


class TestPerformanceIntegration:
    """Test performance characteristics of the integrated system."""
    
    def test_throughput_single_threaded(self, test_cluster):
        """Test single-threaded throughput."""
        client = test_cluster.client
        
        num_operations = 500
        keys = [f"perf_key_{i:04d}" for i in range(num_operations)]
        values = [f"perf_value_{i:04d}" for i in range(num_operations)]
        
        # Measure write throughput
        start_time = time.time()
        successful_writes = 0
        
        for key, value in zip(keys, values):
            if client.set(key, value):
                successful_writes += 1
        
        write_time = time.time() - start_time
        write_throughput = successful_writes / write_time
        
        print(f"Write throughput: {write_throughput:.1f} ops/sec")
        assert write_throughput > 100  # Should achieve at least 100 writes/sec
        
        # Measure read throughput
        start_time = time.time()
        successful_reads = 0
        
        for key in keys:
            if client.get(key) is not None:
                successful_reads += 1
        
        read_time = time.time() - start_time
        read_throughput = successful_reads / read_time
        
        print(f"Read throughput: {read_throughput:.1f} ops/sec")
        assert read_throughput > 200  # Should achieve at least 200 reads/sec
        
        # Check hit rate
        hit_rate = (successful_reads / len(keys)) * 100
        print(f"Hit rate: {hit_rate:.1f}%")
        assert hit_rate > 95  # Should have high hit rate
    
    def test_concurrent_access(self, test_cluster):
        """Test concurrent access from multiple threads."""
        client = test_cluster.client
        
        num_threads = 5
        operations_per_thread = 50
        results = []
        errors = []
        
        def worker_thread(thread_id):
            thread_results = {"reads": 0, "writes": 0, "errors": 0}
            
            try:
                for i in range(operations_per_thread):
                    key = f"thread_{thread_id}_key_{i:03d}"
                    value = f"thread_{thread_id}_value_{i:03d}"
                    
                    # Write
                    if client.set(key, value):
                        thread_results["writes"] += 1
                    else:
                        thread_results["errors"] += 1
                    
                    # Read
                    retrieved = client.get(key)
                    if retrieved == value:
                        thread_results["reads"] += 1
                    else:
                        thread_results["errors"] += 1
                        
            except Exception as e:
                thread_results["errors"] += 1
                errors.append(f"Thread {thread_id}: {e}")
            
            results.append(thread_results)
        
        # Start all threads
        threads = []
        start_time = time.time()
        
        for i in range(num_threads):
            thread = threading.Thread(target=worker_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        total_time = time.time() - start_time
        
        # Analyze results
        total_operations = sum(r["reads"] + r["writes"] for r in results)
        total_errors = sum(r["errors"] for r in results)
        
        print(f"Concurrent test: {total_operations} ops in {total_time:.2f}s")
        print(f"Throughput: {total_operations/total_time:.1f} ops/sec")
        print(f"Error rate: {total_errors}/{total_operations + total_errors}")
        
        # Should have low error rate
        error_rate = total_errors / (total_operations + total_errors) if total_operations + total_errors > 0 else 0
        assert error_rate < 0.05  # Less than 5% error rate
        
        # Should achieve reasonable concurrent throughput
        concurrent_throughput = total_operations / total_time
        assert concurrent_throughput > 200  # At least 200 ops/sec with concurrency


class TestFaultToleranceIntegration:
    """Test system behavior under failure conditions."""
    
    def test_graceful_degradation(self, test_cluster):
        """Test that system degrades gracefully when nodes are unreachable."""
        client = test_cluster.client
        
        # Store some data first
        test_data = {f"fault_test_{i}": f"value_{i}" for i in range(20)}
        
        for key, value in test_data.items():
            client.set(key, value)
        
        # Simulate network issues by modifying client timeout
        original_timeout = client.timeout
        client.timeout = 0.001  # Very short timeout to simulate network issues
        
        try:
            # Try to access data with simulated network issues
            successful_reads = 0
            for key in test_data.keys():
                value = client.get(key)
                if value is not None:
                    successful_reads += 1
            
            # Some reads might fail due to timeout, but system shouldn't crash
            print(f"Successful reads under network stress: {successful_reads}/{len(test_data)}")
            
        finally:
            # Restore original timeout
            client.timeout = original_timeout
        
        # Verify system recovers
        time.sleep(0.1)
        recovered_reads = 0
        for key in test_data.keys():
            value = client.get(key)
            if value is not None:
                recovered_reads += 1
        
        print(f"Reads after recovery: {recovered_reads}/{len(test_data)}")
        assert recovered_reads > successful_reads  # Should recover


class TestClusterManagementIntegration:
    """Test cluster management and monitoring functionality."""
    
    def test_cluster_health_monitoring(self, test_cluster):
        """Test cluster health monitoring."""
        client = test_cluster.client
        
        # Get cluster health
        health = client.get_cluster_health()
        
        assert health["cluster_healthy"] is True
        assert health["healthy_count"] == len(test_cluster.nodes)
        assert health["unhealthy_count"] == 0
        assert len(health["healthy_nodes"]) == len(test_cluster.nodes)
        assert len(health["unhealthy_nodes"]) == 0
        
        # All expected nodes should be healthy
        expected_nodes = {node_id for node_id, _ in test_cluster.nodes}
        healthy_nodes = set(health["healthy_nodes"])
        assert healthy_nodes == expected_nodes
    
    def test_cluster_statistics(self, test_cluster):
        """Test cluster statistics gathering."""
        client = test_cluster.client
        
        # Add some data first
        for i in range(30):
            client.set(f"stats_key_{i:03d}", f"stats_value_{i:03d}")
        
        # Get stats from all nodes
        stats = client.get_stats()
        
        assert len(stats) == len(test_cluster.nodes)
        
        # All nodes should report stats
        for node_id, _ in test_cluster.nodes:
            assert node_id in stats
            node_stats = stats[node_id]
            assert "error" not in node_stats
            assert "total_keys" in node_stats
            assert "memory_usage_estimate" in node_stats
            assert node_stats["total_keys"] >= 0
        
        # Total keys across all nodes should be reasonable
        total_keys = sum(stats[node]["total_keys"] for node, _ in test_cluster.nodes)
        assert total_keys >= 30  # Should have at least the keys we stored
        
        print(f"Cluster stats: {stats}")


if __name__ == "__main__":
    print("=== Integration Tests ===")
    print("These tests require starting actual cache nodes.")
    print("Run with: python -m pytest tests/test_integration.py -v -s")
    print("\nFor manual testing, you can also run individual test components:")
    
    # Test consistent hashing
    ring = ConsistentHashRing(["node1", "node2", "node3"])
    test_keys = [f"test_key_{i}" for i in range(20)]
    
    print(f"\nConsistent hashing test:")
    node_counts = {}
    for key in test_keys:
        node = ring.get_node(key)
        node_counts[node] = node_counts.get(node, 0) + 1
    
    for node, count in sorted(node_counts.items()):
        percentage = (count / len(test_keys)) * 100
        print(f"  {node}: {count:2} keys ({percentage:5.1f}%)")
    
    print("\n✅ Basic integration components working!")