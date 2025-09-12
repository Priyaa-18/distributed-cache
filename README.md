# Distributed Cache System

A production-ready distributed cache implementation using **consistent hashing** for automatic data distribution and fault tolerance. Built to demonstrate core distributed systems concepts used by companies like Amazon (DynamoDB), Netflix, and Facebook.

## 🚀 Quick Start

```bash
# Clone and setup
git clone <your-repo-url>
cd distributed-cache
pip install -r requirements.txt

# Run the full demo (starts cluster automatically)
python src/demo.py
```

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │    │   Client    │    │   Client    │
│ Application │    │ Application │    │ Application │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
              ┌───────────▼───────────┐
              │    Smart Client       │
              │  (Consistent Hash)    │
              └───────────┬───────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
   ┌────▼────┐       ┌────▼────┐       ┌────▼────┐
   │ Node 1  │       │ Node 2  │       │ Node 3  │
   │Port 8001│       │Port 8002│       │Port 8003│
   └─────────┘       └─────────┘       └─────────┘
```

### Key Components

- **Consistent Hash Ring**: Distributes keys evenly across nodes with minimal data movement during scaling
- **Cache Nodes**: HTTP servers storing key-value pairs with TTL support
- **Smart Client**: Automatically routes requests to correct nodes using the hash ring
- **Replication**: Stores multiple copies for fault tolerance

## 🎯 Key Features

- **Distributed**: Scales horizontally by adding more nodes
- **Consistent Hashing**: Adding/removing nodes only affects adjacent data ranges
- **Fault Tolerant**: Continues working even when nodes fail
- **High Performance**: Sub-millisecond lookups with in-memory storage
- **TTL Support**: Automatic expiration of cached data
- **Replication**: Configurable data replication across multiple nodes
- **REST API**: Standard HTTP interface for easy integration

## 📊 Performance Characteristics

- **Throughput**: 1000+ operations/second per node
- **Latency**: < 1ms for cache hits
- **Data Movement**: Only ~25% of data moves when adding a node (vs 100% with naive hashing)
- **Memory Efficient**: Automatic cleanup of expired keys

## 🔧 Manual Usage

### Start Individual Nodes

```bash
# Terminal 1
python src/cache_node.py server1 8001

# Terminal 2  
python src/cache_node.py server2 8002

# Terminal 3
python src/cache_node.py server3 8003
```

### Use the Client

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

### HTTP API

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

## 🧪 Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Test just the hash ring
python tests/test_hash_ring.py

# Test with coverage
pip install pytest-cov
python -m pytest tests/ --cov=src --cov-report=html
```

## 🎮 Demo Scenarios

The `demo.py` script showcases:

1. **Basic Operations**: Store/retrieve data across multiple nodes
2. **Consistent Hashing**: Even distribution and minimal data movement
3. **Fault Tolerance**: System continues working when nodes fail
4. **Performance**: Benchmark throughput and latency
5. **Scaling**: Adding new nodes with minimal disruption

## 🏭 Production Considerations

For production use, consider adding:

- **Persistence**: Write-ahead logs or periodic snapshots
- **Authentication**: API keys or OAuth for security
- **Monitoring**: Prometheus metrics and Grafana dashboards
- **Load Balancing**: Multiple replicas behind a load balancer
- **Configuration**: External config files for cluster topology
- **Logging**: Structured logging with correlation IDs

## 🔬 Consistent Hashing Deep Dive

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

## 📚 Educational Value

This project demonstrates concepts essential for distributed systems:

- **Consistent Hashing**: Core algorithm in Amazon DynamoDB, Apache Cassandra
- **Horizontal Scaling**: Add capacity by adding more nodes  
- **Fault Tolerance**: Handle node failures gracefully
- **Load Distribution**: Even distribution without hotspots
- **CAP Theorem**: Trade-offs between consistency, availability, and partition tolerance

## 🎯 Interview Preparation

Perfect for discussing in system design interviews:

- "How would you design a distributed cache like Redis Cluster?"
- "How do you handle data distribution and replication?"
- "What happens when nodes join or leave the cluster?"
- "How do you ensure high availability?"

## 📈 Scaling

- **Vertical**: Increase memory/CPU on existing nodes
- **Horizontal**: Add more nodes to the ring
- **Replication**: Increase replica count for higher availability
- **Sharding**: Partition data across multiple independent clusters

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

---

*Built with ❤️ to demonstrate distributed systems concepts*