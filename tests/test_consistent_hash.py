"""Simple test for consistent hash."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from consistent_hash import ConsistentHashRing

def test_basic_functionality():
    """Test basic hash ring functionality."""
    ring = ConsistentHashRing(["node1", "node2", "node3"])
    
    # Test that keys get assigned to nodes
    key = "test_key"
    node = ring.get_node(key)
    assert node in ["node1", "node2", "node3"]
    
    # Test consistency
    assert ring.get_node(key) == node
    
    print("✅ Basic hash ring test passed!")

if __name__ == "__main__":
    test_basic_functionality()