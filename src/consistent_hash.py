"""
Consistent Hashing Ring Implementation

This is the core algorithm that makes distributed caching work efficiently.
When nodes are added or removed, only a small portion of data needs to move.
"""

import hashlib
import bisect
from typing import List, Optional, Set


class ConsistentHashRing:
    """
    Consistent hashing ring for distributed cache nodes.
    
    Uses virtual nodes (replicas) to ensure even distribution of data
    across physical nodes, even when the hash function produces
    unevenly distributed hash values.
    """
    
    def __init__(self, nodes: Optional[List[str]] = None, replicas: int = 150):
        """
        Initialize the hash ring.
        
        Args:
            nodes: List of initial node identifiers
            replicas: Number of virtual nodes per physical node (higher = more even distribution)
        """
        self.replicas = replicas
        self.ring = {}  # hash_value -> physical_node_id
        self.sorted_keys = []  # Sorted hash values for efficient lookup
        self.nodes = set()  # Track physical nodes
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def _hash(self, key: str) -> int:
        """
        Hash function that maps keys to positions on the ring.
        
        Uses MD5 for consistent, well-distributed hash values.
        In production, you might want to use a faster hash function.
        """
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
    
    def add_node(self, node_id: str) -> None:
        """
        Add a new node to the ring.
        
        Creates multiple virtual nodes (replicas) for this physical node
        to ensure better data distribution.
        """
        if node_id in self.nodes:
            return  # Node already exists
            
        self.nodes.add(node_id)
        
        # Create virtual nodes
        for i in range(self.replicas):
            virtual_key = f"{node_id}:{i}"
            hash_value = self._hash(virtual_key)
            self.ring[hash_value] = node_id
        
        # Keep sorted keys for efficient lookups
        self.sorted_keys = sorted(self.ring.keys())
        print(f"Added node {node_id} with {self.replicas} virtual nodes")
    
    def remove_node(self, node_id: str) -> None:
        """
        Remove a node from the ring.
        
        All virtual nodes for this physical node are removed.
        Data that was stored on this node will need to be redistributed.
        """
        if node_id not in self.nodes:
            return  # Node doesn't exist
            
        self.nodes.remove(node_id)
        
        # Remove all virtual nodes for this physical node
        keys_to_remove = [key for key, node in self.ring.items() if node == node_id]
        for key in keys_to_remove:
            del self.ring[key]
        
        self.sorted_keys = sorted(self.ring.keys())
        print(f"Removed node {node_id}")
    
    def get_node(self, key: str) -> Optional[str]:
        """
        Find which node should store the given key.
        
        Uses the consistent hashing algorithm:
        1. Hash the key to get a position on the ring
        2. Find the first node clockwise from that position
        3. Return the physical node ID
        """
        if not self.ring:
            return None
        
        hash_value = self._hash(key)
        
        # Find the first node clockwise from our hash value
        # Using bisect for O(log n) lookup instead of O(n)
        idx = bisect.bisect_right(self.sorted_keys, hash_value)
        
        if idx == len(self.sorted_keys):
            # Wrap around to the beginning of the ring
            idx = 0
        
        return self.ring[self.sorted_keys[idx]]
    
    def get_nodes(self, key: str, count: int = 3) -> List[str]:
        """
        Get multiple nodes for replication.
        
        Returns the next 'count' unique physical nodes clockwise from the key's position.
        Useful for storing replicas of the same data on different nodes.
        """
        if not self.ring or count <= 0:
            return []
        
        hash_value = self._hash(key)
        idx = bisect.bisect_right(self.sorted_keys, hash_value)
        
        nodes = []
        seen_physical_nodes = set()
        
        # Collect unique physical nodes
        for i in range(len(self.sorted_keys)):
            current_idx = (idx + i) % len(self.sorted_keys)
            physical_node = self.ring[self.sorted_keys[current_idx]]
            
            if physical_node not in seen_physical_nodes:
                nodes.append(physical_node)
                seen_physical_nodes.add(physical_node)
                
                if len(nodes) == count:
                    break
        
        return nodes
    
    def get_node_load_distribution(self) -> dict:
        """
        Analyze how evenly data would be distributed across nodes.
        
        Returns a dictionary showing what percentage of the hash space
        each node is responsible for. Useful for debugging and monitoring.
        """
        if not self.sorted_keys:
            return {}
        
        node_ranges = {}
        ring_size = 2 ** 128  # MD5 hash space size
        
        for i, key in enumerate(self.sorted_keys):
            node = self.ring[key]
            
            # Calculate the range this virtual node covers
            if i == 0:
                # First node covers from last node to itself
                prev_key = self.sorted_keys[-1]
                range_size = (key + ring_size - prev_key) % ring_size
            else:
                range_size = key - self.sorted_keys[i - 1]
            
            if node not in node_ranges:
                node_ranges[node] = 0
            node_ranges[node] += range_size
        
        # Convert to percentages
        return {node: (size / ring_size) * 100 
                for node, size in node_ranges.items()}
    
    def __str__(self) -> str:
        """String representation showing ring status."""
        if not self.nodes:
            return "Empty hash ring"
        
        distribution = self.get_node_load_distribution()
        lines = [f"Hash ring with {len(self.nodes)} nodes:"]
        for node in sorted(self.nodes):
            percentage = distribution.get(node, 0)
            lines.append(f"  {node}: {percentage:.2f}% of hash space")
        
        return "\n".join(lines)