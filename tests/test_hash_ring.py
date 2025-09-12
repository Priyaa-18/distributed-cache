"""
Tests for the consistent hashing ring.

These tests verify that the core distributed caching algorithm works correctly.
"""

import pytest
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
    
    def test_node_addition_minimal_disruption(self):
        """Test that adding a node only affects a subset of keys."""
        # Start with 2 nodes
        ring = ConsistentHashRing(["node1", "node2"])
        
        # Get initial assignments for many keys
        test_keys = [f"key_{i}" for i in range(1000)]
        initial_assignments = {key: ring.get_node(key) for key in test_keys}
        
        # Add a third node
        ring.add_node("node3")
        
        # Check how many assignments changed
        changed = 0
        for key in test_keys:
            if ring.get_node(key) != initial_assignments[key]:
                changed += 1
        
        # Should be roughly 1/3 of keys (since we went from 2 to 3 nodes)
        # Allow some tolerance due to randomness in hashing
        change_percentage = changed / len(test_keys)
        assert 0.2 < change_percentage < 0.5, f"Expected ~33% change, got {change_percentage:.2%}"
        
        print(f"Adding node3: {change_percentage:.2%} of keys moved")
    
    def test_node_removal(self):
        """Test removing a node from the ring."""
        ring = ConsistentHashRing(["node1", "node2", "node3"])
        
        # Verify node3 exists
        assert "node3" in ring.nodes
        
        # Remove it
        ring.remove_node("node3")
        assert "node3" not in ring.nodes
        
        # All keys should now only go to node1 or node2
        for i in range(100):
            node = ring.get_node(f"key_{i}")
            assert node in ["node1", "node2"]
    
    def test_replication_nodes(self):
        """Test getting multiple nodes for replication."""
        ring = ConsistentHashRing(["node1", "node2", "node3", "node4"])
        
        # Get 3 replica nodes for a key
        replica_nodes = ring.get_nodes("test_key", 3)
        
        assert len(replica_nodes) == 3
        assert len(set(replica_nodes)) == 3  # All unique nodes
        
        # All should be valid nodes
        for node in replica_nodes:
            assert node in ["node1", "node2", "node3", "node4"]
    
    def test_load_distribution(self):
        """Test that load is reasonably distributed across nodes."""
        ring = ConsistentHashRing(["node1", "node2", "node3"], replicas=150)
        
        distribution = ring.get_node_load_distribution()
        
        # Each node should get roughly 33.33% of the load
        for node, percentage in distribution.items():
            assert 25 < percentage < 40, f"Node {node} has {percentage:.2f}% load (should be ~33%)"
        
        # Total should be 100%
        total = sum(distribution.values())
        assert 99.9 < total < 100.1
    
    def test_virtual_nodes_effect(self):
        """Test that more virtual nodes give better distribution."""
        # Test with few virtual nodes
        ring_few = ConsistentHashRing(["node1", "node2", "node3"], replicas=1)
        dist_few = ring_few.get_node_load_distribution()
        
        # Test with many virtual nodes  
        ring_many = ConsistentHashRing(["node1", "node2", "node3"], replicas=150)
        dist_many = ring_many.get_node_load_distribution()
        
        # Calculate variance in distribution
        def variance(distribution):
            mean = sum(distribution.values()) / len(distribution)
            return sum((v - mean) ** 2 for v in distribution.values()) / len(distribution)
        
        var_few = variance(dist_few)
        var_many = variance(dist_many)
        
        # More virtual nodes should have lower variance (better distribution)
        assert var_many < var_few, "More virtual nodes should give better load distribution"
        
        print(f"Variance with 1 replica: {var_few:.2f}")
        print(f"Variance with 150 replicas: {var_many:.2f}")


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
    
    # Add a new node and show minimal disruption
    print("Adding server4...")
    initial_assignments = {key: ring.get_node(key) for key in test_keys}
    ring.add_node("server4")
    
    print("New assignments:")
    changes = 0
    for key in test_keys:
        old_node = initial_assignments[key]
        new_node = ring.get_node(key)
        changed = "MOVED" if old_node != new_node else ""
        print(f"  {key:12} -> {new_node} {changed}")
        if old_node != new_node:
            changes += 1
    
    print(f"\nOnly {changes}/{len(test_keys)} keys moved when adding a new node!")
    print(f"Final distribution:\n{ring}")