"""
Distributed Cache Client

A smart client that uses consistent hashing to automatically route
requests to the correct cache node. This should make distributed caching transparent to application developers.
"""

import json
import requests
import time
from typing import Any, Dict, List, Optional, Tuple
from consistent_hash import ConsistentHashRing


class CacheClient:
    """
    Smart client for the distributed cache.
    
    Automatically routes requests to the correct node based on consistent hashing.
    Handles node failures and provides a simple interface for applications.
    """
    
    def __init__(self, nodes: List[Tuple[str, int]], timeout: float = 5.0):
        """
        Initialize the cache client.
        
        Args:
            nodes: List of (node_id, port) tuples for cache nodes
            timeout: HTTP request timeout in seconds
        """
        self.timeout = timeout
        self.nodes = {}  # node_id -> (host, port)
        self.hash_ring = ConsistentHashRing()
        
        # Add all nodes to the hash ring
        for node_id, port in nodes:
            host = 'localhost'  # Could be configurable for multi-host setups
            self.nodes[node_id] = (host, port)
            self.hash_ring.add_node(node_id)
        
        print(f"Cache client initialized with {len(nodes)} nodes")
        print(f"Hash ring: {list(self.nodes.keys())}")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the distributed cache.
        
        Automatically routes to the correct node based on consistent hashing.
        Returns None if the key doesn't exist or the node is unreachable.
        """
        node_id = self.hash_ring.get_node(key)
        if not node_id:
            raise Exception("No nodes available in hash ring")
        
        host, port = self.nodes[node_id]
        url = f"http://{host}:{port}/cache/{key}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('value')
            elif response.status_code == 404:
                return None  # Key not found
            else:
                print(f"Error getting key '{key}' from {node_id}: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Network error getting key '{key}' from {node_id}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a value in the distributed cache.
        
        Automatically routes to the correct node based on consistent hashing.
        Returns True if successful, False otherwise.
        """
        node_id = self.hash_ring.get_node(key)
        if not node_id:
            raise Exception("No nodes available in hash ring")
        
        host, port = self.nodes[node_id]
        url = f"http://{host}:{port}/cache/{key}"
        
        payload = {"value": value}
        if ttl is not None:
            payload["ttl"] = ttl
        
        try:
            response = requests.post(
                url, 
                json=payload, 
                timeout=self.timeout,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                return True
            else:
                print(f"Error setting key '{key}' on {node_id}: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Network error setting key '{key}' on {node_id}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from the distributed cache.
        
        Returns True if the key was deleted, False if it didn't exist or on error.
        """
        node_id = self.hash_ring.get_node(key)
        if not node_id:
            raise Exception("No nodes available in hash ring")
        
        host, port = self.nodes[node_id]
        url = f"http://{host}:{port}/cache/{key}"
        
        try:
            response = requests.delete(url, timeout=self.timeout)
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                return False  # Key didn't exist
            else:
                print(f"Error deleting key '{key}' from {node_id}: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Network error deleting key '{key}' from {node_id}: {e}")
            return False
    
    def get_with_replication(self, key: str, read_quorum: int = 1) -> Optional[Any]:
        """
        Get a value with replication support.
        
        Tries to read from multiple replica nodes and returns the first successful read.
        Useful for fault tolerance when nodes might be down.
        """
        replica_nodes = self.hash_ring.get_nodes(key, read_quorum + 2)  # Get extra replicas
        
        for node_id in replica_nodes[:read_quorum + 2]:  # Try a few nodes
            if node_id not in self.nodes:
                continue
                
            host, port = self.nodes[node_id]
            url = f"http://{host}:{port}/cache/{key}"
            
            try:
                response = requests.get(url, timeout=self.timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    return data.get('value')
                elif response.status_code == 404:
                    continue  # Try next replica
                    
            except requests.exceptions.RequestException:
                continue  # Try next replica
        
        return None  # No replica had the key or all failed
    
    def set_with_replication(self, key: str, value: Any, replicas: int = 3, ttl: Optional[int] = None) -> int:
        """
        Set a value with replication across multiple nodes.
        
        Returns the number of nodes that successfully stored the value.
        """
        replica_nodes = self.hash_ring.get_nodes(key, replicas)
        successful_writes = 0
        
        payload = {"value": value}
        if ttl is not None:
            payload["ttl"] = ttl
        
        for node_id in replica_nodes:
            if node_id not in self.nodes:
                continue
                
            host, port = self.nodes[node_id]
            url = f"http://{host}:{port}/cache/{key}"
            
            try:
                response = requests.post(
                    url, 
                    json=payload, 
                    timeout=self.timeout,
                    headers={'Content-Type': 'application/json'}
                )
                
                if response.status_code == 200:
                    successful_writes += 1
                    
            except requests.exceptions.RequestException as e:
                print(f"Failed to write to replica {node_id}: {e}")
        
        return successful_writes
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics from all nodes in the cluster."""
        all_stats = {}
        
        for node_id, (host, port) in self.nodes.items():
            url = f"http://{host}:{port}/stats"
            
            try:
                response = requests.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    all_stats[node_id] = response.json()
                else:
                    all_stats[node_id] = {"error": f"HTTP {response.status_code}"}
                    
            except requests.exceptions.RequestException as e:
                all_stats[node_id] = {"error": str(e)}
        
        return all_stats
    
    def get_cluster_health(self) -> Dict[str, Any]:
        """Check the health of all nodes in the cluster."""
        health_status = {
            "healthy_nodes": [],
            "unhealthy_nodes": [],
            "total_nodes": len(self.nodes)
        }
        
        for node_id, (host, port) in self.nodes.items():
            url = f"http://{host}:{port}/health"
            
            try:
                response = requests.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    health_status["healthy_nodes"].append(node_id)
                else:
                    health_status["unhealthy_nodes"].append(node_id)
                    
            except requests.exceptions.RequestException:
                health_status["unhealthy_nodes"].append(node_id)
        
        health_status["healthy_count"] = len(health_status["healthy_nodes"])
        health_status["unhealthy_count"] = len(health_status["unhealthy_nodes"])
        health_status["cluster_healthy"] = health_status["unhealthy_count"] == 0
        
        return health_status
    
    def show_key_distribution(self, keys: List[str]) -> None:
        """Show which node each key would be assigned to (for debugging)."""
        print(f"\nKey distribution across {len(self.nodes)} nodes:")
        print("-" * 50)
        
        node_counts = {node_id: 0 for node_id in self.nodes}
        
        for key in keys:
            node_id = self.hash_ring.get_node(key)
            if node_id:
                node_counts[node_id] += 1
                print(f"{key:20} -> {node_id}")
        
        print("-" * 50)
        print("Summary:")
        for node_id, count in node_counts.items():
            percentage = (count / len(keys)) * 100 if keys else 0
            print(f"{node_id:15}: {count:3} keys ({percentage:5.1f}%)")


def main():
    """
    Demo script showing how to use the distributed cache client.
    
    Make sure you have cache nodes running first:
    python src/cache_node.py server1 8001 &
    python src/cache_node.py server2 8002 &
    python src/cache_node.py server3 8003 &
    """
    
    # Connect to a 3-node cluster
    client = CacheClient([
        ("server1", 8001),
        ("server2", 8002), 
        ("server3", 8003)
    ])
    
    print("\n=== Distributed Cache Demo ===")
    
    # Check cluster health
    health = client.get_cluster_health()
    print(f"\nCluster Health: {health['healthy_count']}/{health['total_nodes']} nodes healthy")
    if not health['cluster_healthy']:
        print(f"Unhealthy nodes: {health['unhealthy_nodes']}")
        print("Make sure all cache nodes are running!")
        return
    
    # Store some data
    print("\n1. Storing data...")
    test_data = {
        "user:123": "Alice Johnson",
        "user:456": "Bob Smith", 
        "session:abc": {"user_id": 123, "login_time": "2024-01-01T10:00:00Z"},
        "cache:popular_posts": ["post1", "post2", "post3"],
        "temp:calculation": 42
    }
    
    for key, value in test_data.items():
        success = client.set(key, value, ttl=3600)  # 1 hour TTL
        node = client.hash_ring.get_node(key)
        print(f"  {key:20} -> {node:8} {'✓' if success else '✗'}")
    
    # Retrieve the data
    print("\n2. Retrieving data...")
    for key in test_data.keys():
        value = client.get(key)
        node = client.hash_ring.get_node(key)
        print(f"  {key:20} -> {node:8} = {value}")
    
    # Show key distribution
    all_keys = list(test_data.keys()) + [f"extra_key_{i}" for i in range(10)]
    client.show_key_distribution(all_keys)
    
    # Get cluster statistics
    print("\n3. Cluster Statistics:")
    stats = client.get_stats()
    for node_id, node_stats in stats.items():
        if "error" not in node_stats:
            print(f"  {node_id:10}: {node_stats['total_keys']} keys, "
                  f"{node_stats['memory_usage_estimate']} bytes")
        else:
            print(f"  {node_id:10}: {node_stats['error']}")
    
    # Test replication
    print("\n4. Testing replication...")
    replicas_written = client.set_with_replication("replicated:key", "important data", replicas=3)
    print(f"Replicated key written to {replicas_written} nodes")
    
    # Test reading with replication
    replicated_value = client.get_with_replication("replicated:key")
    print(f"Retrieved replicated value: {replicated_value}")
    
    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    main()