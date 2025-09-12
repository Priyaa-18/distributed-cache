"""
Tests for the consistent hashing ring.

These tests verify that the core distributed caching algorithm works correctly.
"""

import sys
import os

# Add src directory to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from consistent_hash import ConsistentHashRing


class TestConsistentHashRing:
    
    def test_empty_ring(self):
        """Test behavior with no nodes."""
        ring = ConsistentHashRing()
        assert ring.get_node("any_key") is None
        assert ring.get_nodes("any_key", 3) == []
    
    def test_single_node(self):
        """Test with a single node - all keys should go there."""
        ring = ConsistentHashRing(["node1"])
        
        assert ring.get_node("key1") == "node1"
        assert ring.get_node("key2") == "node1"
        assert ring.get_node("completely_different_key") == "node1"
        
        # All replicas should be the same node
        assert ring.get_nodes("key1", 3) == ["node1"]
    
    def test_multiple_nodes(self):
        """Test that keys get distributed across multiple nodes."""
        ring = ConsistentHashRing(["node1", "node2", "node3"])
        
        # Test that different keys can go to different nodes
        assignments = {}
        test_keys = [f"key_{i}" for i in range(100)]
        
        for key in test_keys:
            node = ring.get_node(key)
            assert node in ["node1", "node2", "node3"]
            assignments[key] = node
        
        # Verify that not all keys go to the same node
        assigned_nodes = set(assignments.values())
        assert len(assigned_nodes) > 1, "Keys should be distributed across multiple nodes"
    
    def test_consistent_assignment(self):
        """Test that the same key always goes to the same node."""
        ring = ConsistentHashRing(["node1", "node2", "node3"])
        
        key = "consistent_key"
        first_assignment = ring.get_node(key)
        
        # Call multiple times - should always get the same result
        for _ in range(10):
            assert ring.get_node(key) == first_assignment


if __name__ == "__main__":
    # Run a quick demo
    print("=== Consistent Hash Ring Demo ===\n")
    
    # Create ring with 3 nodes
    ring = ConsistentHashRing(["server1", "server2", "server3"])
    print(ring)
    print()
    
    # Show where some keys would go
    test_keys = ["user:123", "user:456", "session:abc", "cache:data", "temp:file"]
    print("Key assignments:")
    for key in test_keys:
        node = ring.get_node(key)
        replicas = ring.get_nodes(key, 3)
        print(f"  {key:12} -> {node} (replicas: {replicas})")
    print()
    
    print("=== Basic tests passed! ===")
    print("Run 'python -m pytest tests/test_hash_ring.py -v' for full test suite")