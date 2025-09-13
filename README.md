# Distributed Cache System

A scalable, fault-tolerant, in-memory distributed cache built with consistent hashing, replication, and TTL support. Demonstrates concepts from distributed systems such as horizontal scaling, CAP theorem trade-offs, and load distribution.

## Table of Contents
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Features](#features)
- [Usage](#usage)
- [Testing & Quality Assurance](#testing--quality-assurance)
- [Benchmarked Performance](#benchmarked-performance)
- [Consistent Hashing Deep Dive](#consistent-hashing-deep-dive)
- [Real-World Extensions](#real-world-extensions)
- [Contributing](#contributing)
- [License](#license)

## Quick Start

**Prerequisites:**
- Python 3.7+
- pip

```bash
# Clone and setup
git clone <https://github.com/Priyaa-18/distributed-cache.git>
cd distributed-cache
pip install -r requirements.txt

# Run demo
python src/demo.py
```

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │    │   Client    │    │   Client    │
│ Application │    │ Application │    │ Application │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └───────────┬──────┴───────┬──────────┘
                   │   Requests   │
                   ▼              ▼
          ┌────────────────────────────┐
          │        Smart Client        │
          │        (client.py)         │
          │ - Consistent Hashing       │
          │ - Request Routing          │
          │ - Connection Pooling       │
          │ - Replication Support      │
          └───────────┬────────────────┘
                      │
     ┌────────────────┼───────────────────┐
     │                │                   │
┌────▼─────┐     ┌────▼─────┐       ┌────▼─────┐
│  Node 1  │     │  Node 2  │       │  Node 3  │
│ Port 8001│     │ Port 8002│       │ Port 8003│
│  HTTP API│     │  HTTP API│       │  HTTP API│
│ In-Memory│     │ In-Memory│       │ In-Memory│
│  Storage │     │  Storage │       │  Storage │
│  + TTL   │     │  + TTL   │       │  + TTL   │
│/health   │     │/health   │       │/health   │
│/stats    │     │/stats    │       │/stats    │
└────┬─────┘     └────┬─────┘       └────┬─────┘
     │Replication      │Replication       │Replication
     ▼                 ▼                  ▼
 ┌─────────┐      ┌─────────┐        ┌─────────┐
 │Replica 2│      │Replica 3│        │Replica 1│
 │ of N1   │      │ of N2   │        │ of N3   │
 └─────────┘      └─────────┘        └─────────┘
```

### Key Components

- **Consistent Hash Ring**: Distributes keys evenly across nodes with minimal data movement during scaling
- **Cache Nodes**: HTTP servers storing key-value pairs with TTL support
- **Smart Client**: Automatically routes requests to correct nodes using the hash ring
- **Replication**: Stores multiple copies for fault tolerance

## Features

- **Distributed**: Scales horizontally by adding more nodes
- **Consistent Hashing**: Adding/removing nodes only affects adjacent data ranges
- **Fault Tolerant**: Continues working even when nodes fail
- **High Performance**: Sub-millisecond lookups with in-memory storage
- **TTL Support**: Automatic expiration of cached data
- **Replication**: Configurable data replication across multiple nodes
- **REST API**: Standard HTTP interface for easy integration
- **Testing**: Unit tests and integration testing approaches

## Usage

### Start Individual Nodes

```bash
# Terminal 1
python src/cache_node.py server1 8001

# Terminal 2  
python src/cache_node.py server2 8002

# Terminal 3
python src/cache_node.py server3 8003
```

### Use the Python Client

```python
from src.client import CacheClient

# Connect to cluster
client = CacheClient([
    ("server1", 8001),
    ("server2", 8002), 
    ("server3", 8003)
])

# Basic operations
client.set("user:123", {"name": "Alice", "age": 30})
user = client.get("user:123")
client.delete("user:123")

# With TTL (expires in 1 hour)
client.set("session:abc", "session_data", ttl=3600)

# Replication for fault tolerance
client.set_with_replication("critical:data", "important", replicas=3)
```

### Use the HTTP API

```bash
# Store data
curl -X POST http://localhost:8001/cache/user:123 \
  -H "Content-Type: application/json" \
  -d '{"value": "Alice", "ttl": 3600}'

# Retrieve data
curl http://localhost:8001/cache/user:123

# Delete data
curl -X DELETE http://localhost:8001/cache/user:123

# Get node stats
curl http://localhost:8001/stats

# Health check
curl http://localhost:8001/health
```

## Testing & Quality Assurance

### Quick Commands

```bash
# Install dependencies
make install

# Run all unit tests (fast)
make test-unit

# Run complete test suite
make test

# Run system demo
make run-demo
```

### Test Coverage

**Unit Tests (`test_hash_ring.py`)**
- Consistent hashing algorithm correctness
- Core hash ring functionality and load distribution
- Data movement verification when scaling

**Integration/Demo Tests (`demo.py`)** 
- Complete system demonstration
- Multi-node cluster setup and management
- Fault tolerance simulation
- End-to-end workflow testing

### Running Specific Tests
```bash
# Test hash ring algorithm
python tests/test_hash_ring.py

# Run complete system demo
python src/demo.py
```

### Benchmarking Script

```python3 -c "
import sys
sys.path.append('src')
from consistent_hash import ConsistentHashRing
import time

print('Testing hash ring performance...')
ring = ConsistentHashRing(['node1', 'node2', 'node3'])
keys = [f'key_{i:06d}' for i in range(100000)]

start = time.time()
for key in keys:
    ring.get_node(key)
duration = time.time() - start

print(f'Hash ring lookups: {len(keys)/duration:,.0f} ops/sec')
print(f'Average latency: {(duration/len(keys))*1000:.4f} ms')
"
```

## Benchmarked Performance

**Consistent Hashing Core:**
- 1.07M+ hash ring lookups per second
- Sub-millisecond latency (0.0009ms average)
- 23.0% data movement when scaling (near-optimal)

**Cache Node Operations:**
- 669 writes/second via HTTP
- 704 reads/second via HTTP  
- Thread-safe concurrent access

**Scaling Characteristics:**
- Adding nodes causes minimal data redistribution
- 92% efficiency compared to theoretical optimum
- O(log n) lookup complexity

## Consistent Hashing Deep Dive

The heart of this system is the consistent hashing algorithm:

```python
# Keys get mapped to positions on a ring (0 to 2^32-1)
hash("user:123") = 1,847,291 → goes to next node clockwise

# Virtual nodes ensure even distribution
server1: [100, 523, 891, 1205, ...] (150 positions)
server2: [87, 445, 739, 1456, ...]  (150 positions)  
server3: [234, 667, 1023, 1789, ...](150 positions)
```

When adding a new server, only keys between specific ranges need to move, not the entire dataset.

## Real-World Extensions

### Production Considerations

- **Persistence**: Write-ahead logs or periodic snapshots
- **Authentication**: API keys or OAuth for security
- **Monitoring**: Prometheus metrics and Grafana dashboards
- **Load Balancing**: Multiple replicas behind a load balancer
- **Configuration**: External config files for cluster topology
- **Logging**: Structured logging with correlation IDs

### Scaling

- **Vertical**: Increase memory/CPU on existing nodes
- **Horizontal**: Add more nodes to the ring
- **Replication**: Increase replica count for higher availability
- **Sharding**: Partition data across multiple independent clusters

### Project Value

- **Consistent Hashing**: Core algorithm in Amazon DynamoDB, Apache Cassandra
- **Horizontal Scaling**: Add capacity by adding more nodes  
- **Fault Tolerance**: Handle node failures gracefully
- **CAP Theorem**: Trade-offs between consistency, availability, and partition tolerance

## Contributing

1. Fork the repo
2. Create a feature branch
3. Add tests for new features
4. Ensure all tests pass
5. Submit a PR

## License

MIT License - see LICENSE file for details.

---
