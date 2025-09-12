"""
Unit tests for the distributed cache client.

Tests client functionality, request routing, fault tolerance, and replication.
"""

import pytest
import requests
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from client import CacheClient
from consistent_hash import ConsistentHashRing


class TestCacheClientEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_timeout_configuration(self):
        """Test that timeout is properly configured."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes, timeout=10.0)
        
        assert client.timeout == 10.0
    
    @patch('requests.get')
    def test_get_with_custom_timeout(self, mock_get):
        """Test that requests use the configured timeout."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes, timeout=5.0)
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": "test"}
        mock_get.return_value = mock_response
        
        client.get("test_key")
        
        # Check that timeout was passed to requests.get
        call_args = mock_get.call_args
        assert call_args[1]["timeout"] == 5.0
    
    def test_complex_data_serialization(self):
        """Test handling of complex data types."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        complex_data = {
            "string": "test",
            "number": 42,
            "boolean": True,
            "null": None,
            "array": [1, 2, 3],
            "nested": {"key": "value"}
        }
        
        # This should not raise an exception during JSON serialization
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response
            
            result = client.set("complex_key", complex_data)
            assert result is True
            
            # Verify the data was properly serialized in the request
            call_args = mock_post.call_args
            assert call_args[1]["json"]["value"] == complex_data
    
    def test_unicode_handling(self):
        """Test handling of unicode characters."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        unicode_data = "Hello 世界 🌍 مرحبا العالم"
        
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response
            
            result = client.set("unicode_key", unicode_data)
            assert result is True
    
    def test_large_data_handling(self):
        """Test handling of large data values."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        # Create a large string (1MB)
        large_data = "x" * (1024 * 1024)
        
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response
            
            result = client.set("large_key", large_data)
            assert result is True
    
    @patch('requests.get')
    def test_malformed_response_handling(self, mock_get):
        """Test handling of malformed JSON responses."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response
        
        # Should handle the error gracefully
        result = client.get("test_key")
        assert result is None
    
    def test_empty_string_key(self):
        """Test handling of empty string keys."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        # Should not crash with empty key
        node = client.hash_ring.get_node("")
        assert node in ["node1"]  # Should map to some node
        
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response
            
            result = client.set("", "empty_key_value")
            assert result is True


class TestCacheClientIntegration:
    """Integration tests that verify the complete client workflow."""
    
    def test_consistent_routing(self):
        """Test that the same key always routes to the same node."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        test_key = "consistent_routing_test"
        
        # Get the target node for this key
        target_node = client.hash_ring.get_node(test_key)
        
        # Verify it's consistent across multiple calls
        for _ in range(10):
            assert client.hash_ring.get_node(test_key) == target_node
    
    def test_load_distribution(self):
        """Test that keys are reasonably distributed across nodes."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        # Generate many keys and see their distribution
        num_keys = 300
        test_keys = [f"load_test_key_{i:04d}" for i in range(num_keys)]
        
        node_counts = {}
        for key in test_keys:
            node = client.hash_ring.get_node(key)
            node_counts[node] = node_counts.get(node, 0) + 1
        
        # Each node should get roughly 1/3 of the keys (allow some variance)
        expected_per_node = num_keys / 3
        for node, count in node_counts.items():
            ratio = count / expected_per_node
            assert 0.7 < ratio < 1.3, f"Node {node} got {count} keys, expected ~{expected_per_node}"
    
    def test_replica_selection(self):
        """Test that replica selection returns unique nodes."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003), ("node4", 8004)]
        client = CacheClient(nodes)
        
        test_key = "replica_test_key"
        replicas = client.hash_ring.get_nodes(test_key, 3)
        
        # Should get 3 unique nodes
        assert len(replicas) == 3
        assert len(set(replicas)) == 3  # All unique
        
        # All should be valid node IDs
        for replica in replicas:
            assert replica in ["node1", "node2", "node3", "node4"]
    
    def test_full_workflow_simulation(self):
        """Simulate a complete workflow with multiple operations."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        with patch('requests.post') as mock_post, \
             patch('requests.get') as mock_get, \
             patch('requests.delete') as mock_delete:
            
            # Mock successful responses
            mock_post.return_value = Mock(status_code=200)
            mock_get.return_value = Mock(
                status_code=200,
                **{"json.return_value": {"value": "test_value"}}
            )
            mock_delete.return_value = Mock(status_code=200)
            
            # Simulate application workflow
            test_data = {
                "user:123": {"name": "Alice", "email": "alice@example.com"},
                "session:abc": "session_token_data",
                "cache:popular_posts": [1, 2, 3, 4, 5]
            }
            
            # Store all data
            for key, value in test_data.items():
                result = client.set(key, value, ttl=3600)
                assert result is True
            
            # Retrieve all data
            for key in test_data.keys():
                result = client.get(key)
                assert result == "test_value"  # Mocked response
            
            # Delete all data
            for key in test_data.keys():
                result = client.delete(key)
                assert result is True
            
            # Verify correct number of calls were made
            assert mock_post.call_count == len(test_data)
            assert mock_get.call_count == len(test_data)
            assert mock_delete.call_count == len(test_data)


if __name__ == "__main__":
    print("=== Running Cache Client Tests ===")
    
    # Test basic client functionality
    nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
    client = CacheClient(nodes)
    
    print(f"✅ Client initialized with {len(client.nodes)} nodes")
    
    # Test hash ring integration
    test_keys = ["user:123", "session:abc", "cache:data"]
    print("\n✅ Key routing:")
    for key in test_keys:
        node = client.hash_ring.get_node(key)
        print(f"  {key:12} -> {node}")
    
    # Test load distribution
    many_keys = [f"key_{i:03d}" for i in range(30)]
    node_counts = {}
    for key in many_keys:
        node = client.hash_ring.get_node(key)
        node_counts[node] = node_counts.get(node, 0) + 1
    
    print(f"\n✅ Load distribution across {len(many_keys)} keys:")
    for node, count in sorted(node_counts.items()):
        percentage = (count / len(many_keys)) * 100
        print(f"  {node}: {count:2} keys ({percentage:5.1f}%)")
    
    print("\n=== Basic tests passed! ===")
    print("Run 'python -m pytest tests/test_client.py -v' for full test suite")Client:
    """Test the cache client functionality."""
    
    def test_client_initialization(self):
        """Test client initialization with nodes."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        assert len(client.nodes) == 3
        assert client.nodes["node1"] == ("localhost", 8001)
        assert client.nodes["node2"] == ("localhost", 8002)
        assert client.nodes["node3"] == ("localhost", 8003)
        
        # Check hash ring has all nodes
        assert "node1" in client.hash_ring.nodes
        assert "node2" in client.hash_ring.nodes
        assert "node3" in client.hash_ring.nodes
    
    def test_empty_client(self):
        """Test behavior with no nodes."""
        client = CacheClient([])
        
        with pytest.raises(Exception, match="No nodes available"):
            client.get("any_key")
        
        with pytest.raises(Exception, match="No nodes available"):
            client.set("any_key", "any_value")
    
    @patch('requests.get')
    def test_get_success(self, mock_get):
        """Test successful GET operation."""
        # Setup
        nodes = [("node1", 8001), ("node2", 8002)]
        client = CacheClient(nodes)
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"key": "test_key", "value": "test_value"}
        mock_get.return_value = mock_response
        
        # Test
        result = client.get("test_key")
        
        # Verify
        assert result == "test_value"
        mock_get.assert_called_once()
        
        # Check that correct URL was called
        called_url = mock_get.call_args[0][0]
        assert "cache/test_key" in called_url
    
    @patch('requests.get')
    def test_get_not_found(self, mock_get):
        """Test GET operation when key doesn't exist."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        # Mock 404 response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        result = client.get("nonexistent_key")
        assert result is None
    
    @patch('requests.get')
    def test_get_network_error(self, mock_get):
        """Test GET operation with network error."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        # Mock network error
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        
        result = client.get("test_key")
        assert result is None
    
    @patch('requests.post')
    def test_set_success(self, mock_post):
        """Test successful SET operation."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        result = client.set("test_key", "test_value")
        
        assert result is True
        mock_post.assert_called_once()
        
        # Check request payload
        call_args = mock_post.call_args
        assert call_args[1]["json"] == {"value": "test_value"}
    
    @patch('requests.post')
    def test_set_with_ttl(self, mock_post):
        """Test SET operation with TTL."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        result = client.set("test_key", "test_value", ttl=3600)
        
        assert result is True
        
        # Check TTL was included in payload
        call_args = mock_post.call_args
        expected_payload = {"value": "test_value", "ttl": 3600}
        assert call_args[1]["json"] == expected_payload
    
    @patch('requests.post')
    def test_set_failure(self, mock_post):
        """Test SET operation failure."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        # Mock error response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_post.return_value = mock_response
        
        result = client.set("test_key", "test_value")
        assert result is False
    
    @patch('requests.delete')
    def test_delete_success(self, mock_delete):
        """Test successful DELETE operation."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_delete.return_value = mock_response
        
        result = client.delete("test_key")
        
        assert result is True
        mock_delete.assert_called_once()
    
    @patch('requests.delete')
    def test_delete_not_found(self, mock_delete):
        """Test DELETE operation when key doesn't exist."""
        nodes = [("node1", 8001)]
        client = CacheClient(nodes)
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_delete.return_value = mock_response
        
        result = client.delete("nonexistent_key")
        assert result is False
    
    def test_key_routing(self):
        """Test that keys are routed to correct nodes."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        # Test that same key always goes to same node
        key = "test_key"
        node1 = client.hash_ring.get_node(key)
        node2 = client.hash_ring.get_node(key)
        assert node1 == node2
        
        # Test that different keys can go to different nodes
        different_keys = [f"key_{i}" for i in range(100)]
        assigned_nodes = {client.hash_ring.get_node(key) for key in different_keys}
        
        # Should use multiple nodes (not all keys to same node)
        assert len(assigned_nodes) > 1


class TestCacheClientReplication:
    """Test replication functionality."""
    
    @patch('requests.get')
    def test_get_with_replication_success(self, mock_get):
        """Test get with replication when first node responds."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        # Mock successful response from first try
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": "test_value"}
        mock_get.return_value = mock_response
        
        result = client.get_with_replication("test_key", read_quorum=1)
        
        assert result == "test_value"
        assert mock_get.call_count == 1  # Should stop after first success
    
    @patch('requests.get')
    def test_get_with_replication_fallback(self, mock_get):
        """Test get with replication when first node fails."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        # Mock first call fails, second succeeds
        responses = [
            requests.exceptions.RequestException("Network error"),
            Mock(status_code=200, **{"json.return_value": {"value": "test_value"}})
        ]
        mock_get.side_effect = responses
        
        result = client.get_with_replication("test_key", read_quorum=1)
        
        assert result == "test_value"
        assert mock_get.call_count == 2  # Tried two nodes
    
    @patch('requests.get')
    def test_get_with_replication_all_fail(self, mock_get):
        """Test get with replication when all nodes fail."""
        nodes = [("node1", 8001), ("node2", 8002)]
        client = CacheClient(nodes)
        
        # All calls fail
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        
        result = client.get_with_replication("test_key", read_quorum=1)
        
        assert result is None
        assert mock_get.call_count >= 2  # Tried multiple nodes
    
    @patch('requests.post')
    def test_set_with_replication(self, mock_post):
        """Test set with replication across multiple nodes."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        # Mock successful responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        # Set with 3 replicas
        successful_writes = client.set_with_replication("test_key", "test_value", replicas=3)
        
        assert successful_writes == 3
        assert mock_post.call_count == 3  # Called for each replica
    
    @patch('requests.post')
    def test_set_with_replication_partial_failure(self, mock_post):
        """Test set with replication when some nodes fail."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        # Mock mixed responses (some succeed, some fail)
        responses = [
            Mock(status_code=200),  # Success
            Mock(status_code=500),  # Failure
            Mock(status_code=200),  # Success
        ]
        mock_post.side_effect = responses
        
        successful_writes = client.set_with_replication("test_key", "test_value", replicas=3)
        
        assert successful_writes == 2  # 2 out of 3 succeeded
        assert mock_post.call_count == 3


class TestCacheClientClusterManagement:
    """Test cluster health and stats functionality."""
    
    @patch('requests.get')
    def test_get_stats_success(self, mock_get):
        """Test getting stats from all nodes."""
        nodes = [("node1", 8001), ("node2", 8002)]
        client = CacheClient(nodes)
        
        # Mock responses with different stats
        responses = [
            Mock(status_code=200, **{"json.return_value": {"total_keys": 10, "node_id": "node1"}}),
            Mock(status_code=200, **{"json.return_value": {"total_keys": 5, "node_id": "node2"}})
        ]
        mock_get.side_effect = responses
        
        stats = client.get_stats()
        
        assert len(stats) == 2
        assert stats["node1"]["total_keys"] == 10
        assert stats["node2"]["total_keys"] == 5
        assert mock_get.call_count == 2
    
    @patch('requests.get')
    def test_get_stats_partial_failure(self, mock_get):
        """Test getting stats when some nodes are down."""
        nodes = [("node1", 8001), ("node2", 8002)]
        client = CacheClient(nodes)
        
        # Mock one success, one failure
        responses = [
            Mock(status_code=200, **{"json.return_value": {"total_keys": 10}}),
            requests.exceptions.RequestException("Network error")
        ]
        mock_get.side_effect = responses
        
        stats = client.get_stats()
        
        assert len(stats) == 2
        assert "total_keys" in stats["node1"]
        assert "error" in stats["node2"]
    
    @patch('requests.get')
    def test_get_cluster_health_all_healthy(self, mock_get):
        """Test cluster health when all nodes are healthy."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        # Mock all healthy responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        health = client.get_cluster_health()
        
        assert health["cluster_healthy"] is True
        assert health["healthy_count"] == 3
        assert health["unhealthy_count"] == 0
        assert len(health["healthy_nodes"]) == 3
        assert len(health["unhealthy_nodes"]) == 0
    
    @patch('requests.get')
    def test_get_cluster_health_partial_failure(self, mock_get):
        """Test cluster health with some nodes down."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        # Mock mixed responses
        responses = [
            Mock(status_code=200),  # node1 healthy
            requests.exceptions.RequestException("Network error"),  # node2 down
            Mock(status_code=500),  # node3 error
        ]
        mock_get.side_effect = responses
        
        health = client.get_cluster_health()
        
        assert health["cluster_healthy"] is False
        assert health["healthy_count"] == 1
        assert health["unhealthy_count"] == 2
        assert "node1" in health["healthy_nodes"]
        assert "node2" in health["unhealthy_nodes"]
        assert "node3" in health["unhealthy_nodes"]
    
    def test_show_key_distribution(self, capsys):
        """Test key distribution display."""
        nodes = [("node1", 8001), ("node2", 8002), ("node3", 8003)]
        client = CacheClient(nodes)
        
        test_keys = [f"key_{i}" for i in range(10)]
        
        # This should print distribution without errors
        client.show_key_distribution(test_keys)
        
        # Capture output
        captured = capsys.readouterr()
        assert "Key distribution" in captured.out
        assert "Summary:" in captured.out
        
        # Should show all nodes
        for node_id in ["node1", "node2", "node3"]:
            assert node_id in captured.out


class TestCache