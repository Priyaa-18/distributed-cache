#!/usr/bin/env python3
"""
Distributed Cache Demo

This script demonstrates the full distributed cache system working:
1. Starts multiple cache nodes automatically
2. Shows consistent hashing in action
3. Demonstrates fault tolerance
4. Shows performance characteristics

Run this to see the entire system working end-to-end!
"""

import subprocess
import time
import signal
import os
import sys
from typing import List
from client import CacheClient


class CacheClusterManager:
    """Manages a cluster of cache nodes for demo purposes."""
    
    def __init__(self):
        self.processes: List[subprocess.Popen] = []
        self.nodes = [
            ("server1", 8001),
            ("server2", 8002), 
            ("server3", 8003)
        ]
    
    def start_cluster(self) -> None:
        """Start all cache nodes."""
        print("🚀 Starting distributed cache cluster...")
        
        for node_id, port in self.nodes:
            print(f"  Starting {node_id} on port {port}")
            
            # Start cache node in a subprocess
            process = subprocess.Popen(
                [sys.executable, "src/cache_node.py", node_id, str(port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
            
            self.processes.append(process)
        
        # Give nodes time to start
        print("  Waiting for nodes to start...")
        time.sleep(3)
        
        # Verify all nodes are running
        healthy_nodes = self._check_cluster_health()
        if len(healthy_nodes) == len(self.nodes):
            print(f"✅ All {len(self.nodes)} nodes started successfully!")
        else:
            print(f"⚠️  Only {len(healthy_nodes)}/{len(self.nodes)} nodes are healthy")
    
    def stop_cluster(self) -> None:
        """Stop all cache nodes."""
        print("\n🛑 Stopping cluster...")
        
        for i, process in enumerate(self.processes):
            if process.poll() is None:  # Process is still running
                node_id = self.nodes[i][0]
                print(f"  Stopping {node_id}")
                
                try:
                    if os.name != 'nt':
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    else:
                        process.terminate()
                    process.wait(timeout=5)
                except (subprocess.TimeoutExpired, ProcessLookupError):
                    if os.name != 'nt':
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    else:
                        process.kill()
        
        self.processes.clear()
        print("✅ Cluster stopped")
    
    def _check_cluster_health(self) -> List[str]:
        """Check which nodes are healthy."""
        try:
            client = CacheClient(self.nodes, timeout=2.0)
            health = client.get_cluster_health()
            return health['healthy_nodes']
        except:
            return []
    
    def simulate_node_failure(self, node_index: int) -> None:
        """Simulate a node failure for fault tolerance demo."""
        if 0 <= node_index < len(self.processes):
            process = self.processes[node_index]
            node_id = self.nodes[node_index][0]
            
            if process.poll() is None:
                print(f"💥 Simulating failure of {node_id}")
                process.terminate()
                time.sleep(1)
    
    def restart_node(self, node_index: int) -> None:
        """Restart a failed node."""
        if 0 <= node_index < len(self.nodes):
            node_id, port = self.nodes[node_index]
            print(f"🔄 Restarting {node_id}")
            
            # Start new process
            process = subprocess.Popen(
                [sys.executable, "src/cache_node.py", node_id, str(port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
            
            self.processes[node_index] = process
            time.sleep(2)


def demo_basic_operations(client: CacheClient) -> None:
    """Demonstrate basic cache operations."""
    print("\n📝 Demo 1: Basic Operations")
    print("=" * 40)
    
    # Test data
    test_data = {
        "user:alice": {"name": "Alice", "age": 30, "city": "NYC"},
        "user:bob": {"name": "Bob", "age": 25, "city": "SF"},
        "session:123": "active_session_data",
        "cache:popular": ["item1", "item2", "item3"],
        "counter:visits": 42
    }
    
    print("Storing data across the cluster...")
    for key, value in test_data.items():
        node = client.hash_ring.get_node(key)
        success = client.set(key, value)
        status = "✅" if success else "❌"
        print(f"  {key:15} -> {node:8} {status}")
    
    print("\nRetrieving data...")
    for key in test_data.keys():
        value = client.get(key)
        node = client.hash_ring.get_node(key)
        print(f"  {key:15} -> {node:8} = {str(value)[:30]}...")


def demo_consistent_hashing(client: CacheClient) -> None:
    """Demonstrate consistent hashing properties."""
    print("\n🔄 Demo 2: Consistent Hashing")
    print("=" * 40)
    
    # Show distribution of many keys
    test_keys = [f"key_{i:03d}" for i in range(50)]
    
    print("Key distribution across nodes:")
    node_counts = {}
    for key in test_keys:
        node = client.hash_ring.get_node(key)
        node_counts[node] = node_counts.get(node, 0) + 1
    
    for node, count in sorted(node_counts.items()):
        percentage = (count / len(test_keys)) * 100
        bar = "█" * int(percentage / 2)
        print(f"  {node:8}: {count:2} keys ({percentage:5.1f}%) {bar}")
    
    # Store all test keys
    print(f"\nStoring {len(test_keys)} keys...")
    for i, key in enumerate(test_keys):
        client.set(key, f"value_{i}")
    
    # Show hash ring load distribution
    print("\nHash ring load distribution:")
    distribution = client.hash_ring.get_node_load_distribution()
    for node, percentage in sorted(distribution.items()):
        bar = "█" * int(percentage)
        print(f"  {node:8}: {percentage:5.2f}% of hash space {bar}")


def demo_fault_tolerance(client: CacheClient, cluster_manager: CacheClusterManager) -> None:
    """Demonstrate fault tolerance and replication."""
    print("\n🛡️  Demo 3: Fault Tolerance")
    print("=" * 40)
    
    # Store data with replication
    important_data = {
        "critical:config": {"db_url": "postgres://...", "api_key": "secret"},
        "user:vip": {"name": "VIP User", "tier": "platinum"},
        "session:important": "critical_session_data"
    }
    
    print("Storing data with replication (3 copies each)...")
    for key, value in important_data.items():
        replicas = client.set_with_replication(key, value, replicas=3)
        replica_nodes = client.hash_ring.get_nodes(key, 3)
        print(f"  {key:18} -> {replicas}/3 replicas on {replica_nodes}")
    
    # Simulate node failure
    print("\n💥 Simulating server1 failure...")
    cluster_manager.simulate_node_failure(0)  # Kill server1
    time.sleep(2)
    
    # Check cluster health
    health = client.get_cluster_health()
    print(f"Cluster status: {health['healthy_count']}/{health['total_nodes']} nodes healthy")
    print(f"Failed nodes: {health['unhealthy_nodes']}")
    
    # Try to read data despite the failure
    print("\n🔍 Reading data despite node failure...")
    for key in important_data.keys():
        # Try normal read first
        value = client.get(key)
        if value is None:
            # Fall back to replicated read
            value = client.get_with_replication(key, read_quorum=1)
        
        status = "✅ Recovered" if value else "❌ Lost"
        print(f"  {key:18}: {status}")
    
    # Restart the failed node
    print("\n🔄 Restarting failed node...")
    cluster_manager.restart_node(0)
    time.sleep(3)
    
    health = client.get_cluster_health()
    print(f"Cluster recovered: {health['healthy_count']}/{health['total_nodes']} nodes healthy")


def demo_performance(client: CacheClient) -> None:
    """Demonstrate performance characteristics."""
    print("\n⚡ Demo 4: Performance")
    print("=" * 40)
    
    import random
    
    # Generate test data
    num_operations = 1000
    keys = [f"perf_test_{i:04d}" for i in range(num_operations)]
    values = [f"data_value_{i}" for i in range(num_operations)]
    
    # Benchmark writes
    print(f"Writing {num_operations} keys...")
    start_time = time.time()
    
    successful_writes = 0
    for key, value in zip(keys, values):
        if client.set(key, value):
            successful_writes += 1
    
    write_time = time.time() - start_time
    write_ops_per_sec = successful_writes / write_time
    
    print(f"  Completed: {successful_writes}/{num_operations} writes")
    print(f"  Time: {write_time:.2f} seconds")
    print(f"  Throughput: {write_ops_per_sec:.0f} writes/sec")
    
    # Benchmark reads
    print(f"\nReading {num_operations} keys...")
    random.shuffle(keys)  # Random access pattern
    start_time = time.time()
    
    successful_reads = 0
    cache_hits = 0
    for key in keys:
        value = client.get(key)
        successful_reads += 1
        if value is not None:
            cache_hits += 1
    
    read_time = time.time() - start_time
    read_ops_per_sec = successful_reads / read_time
    hit_rate = (cache_hits / successful_reads) * 100
    
    print(f"  Completed: {successful_reads}/{num_operations} reads")
    print(f"  Time: {read_time:.2f} seconds")
    print(f"  Throughput: {read_ops_per_sec:.0f} reads/sec")
    print(f"  Hit rate: {hit_rate:.1f}%")
    
    # Show cluster statistics
    print("\nCluster statistics:")
    stats = client.get_stats()
    total_keys = 0
    total_memory = 0
    
    for node_id, node_stats in stats.items():
        if "error" not in node_stats:
            keys_count = node_stats['total_keys']
            memory = node_stats['memory_usage_estimate']
            total_keys += keys_count
            total_memory += memory
            print(f"  {node_id:8}: {keys_count:4} keys, {memory:6} bytes")
        else:
            print(f"  {node_id:8}: ERROR - {node_stats['error']}")
    
    print(f"  Total:   {total_keys:4} keys, {total_memory:6} bytes")


def demo_adding_node(client: CacheClient, cluster_manager: CacheClusterManager) -> None:
    """Demonstrate adding a new node to the cluster."""
    print("\n➕ Demo 5: Adding New Node")
    print("=" * 40)
    
    # Show current distribution
    test_keys = [f"distribution_test_{i:03d}" for i in range(100)]
    
    print("Current key distribution:")
    node_counts_before = {}
    for key in test_keys:
        node = client.hash_ring.get_node(key)
        node_counts_before[node] = node_counts_before.get(node, 0) + 1
    
    for node, count in sorted(node_counts_before.items()):
        percentage = (count / len(test_keys)) * 100
        print(f"  {node:8}: {count:2} keys ({percentage:5.1f}%)")
    
    # Add a new node to the hash ring
    print("\n🆕 Adding server4 to the cluster...")
    client.hash_ring.add_node("server4")
    
    # Show new distribution
    print("New key distribution:")
    node_counts_after = {}
    moved_keys = 0
    
    for key in test_keys:
        old_node = client.hash_ring.get_node(key)
        # Temporarily remove server4 to see where it was before
        client.hash_ring.remove_node("server4")
        original_node = client.hash_ring.get_node(key)
        client.hash_ring.add_node("server4")  # Add it back
        
        node_counts_after[old_node] = node_counts_after.get(old_node, 0) + 1
        
        if old_node != original_node:
            moved_keys += 1
    
    for node, count in sorted(node_counts_after.items()):
        percentage = (count / len(test_keys)) * 100
        print(f"  {node:8}: {count:2} keys ({percentage:5.1f}%)")
    
    movement_percentage = (moved_keys / len(test_keys)) * 100
    print(f"\n📊 Data movement: {moved_keys}/{len(test_keys)} keys ({movement_percentage:.1f}%)")
    print("This demonstrates minimal data movement with consistent hashing!")


def main():
    """Run the complete distributed cache demo."""
    cluster_manager = CacheClusterManager()
    
    try:
        # Start the cluster
        cluster_manager.start_cluster()
        
        # Create client
        client = CacheClient([
            ("server1", 8001),
            ("server2", 8002),
            ("server3", 8003)
        ])
        
        # Wait for cluster to be ready
        print("🔍 Checking cluster health...")
        for attempt in range(10):
            health = client.get_cluster_health()
            if health['cluster_healthy']:
                break
            print(f"  Attempt {attempt + 1}: {health['healthy_count']}/3 nodes ready")
            time.sleep(1)
        else:
            print("❌ Cluster failed to start properly")
            return
        
        print("✅ Cluster is healthy and ready!")
        
        # Run all demos
        demo_basic_operations(client)
        demo_consistent_hashing(client)
        demo_fault_tolerance(client, cluster_manager)
        demo_performance(client)
        demo_adding_node(client, cluster_manager)
        
        print("\n🎉 All demos completed successfully!")
        print("\nKey takeaways:")
        print("  • Consistent hashing distributes data evenly")
        print("  • Adding/removing nodes causes minimal data movement") 
        print("  • Replication provides fault tolerance")
        print("  • The system scales horizontally")
        print("  • Client automatically routes to correct nodes")
        
        input("\nPress Enter to stop the cluster...")
        
    except KeyboardInterrupt:
        print("\n⚠️  Demo interrupted")
    except Exception as e:
        print(f"❌ Demo failed: {e}")
    finally:
        cluster_manager.stop_cluster()


if __name__ == "__main__":
    main()