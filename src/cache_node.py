"""
Cache Node Implementation

A simple HTTP server that stores key-value pairs in memory.
Multiple cache nodes can run on different ports to form a distributed cache cluster.
"""

import json
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Dict, Optional, Any


class CacheStorage:
    """
    In-memory storage with TTL (Time To Live) support.
    
    This is where the actual cached data lives. In a production system,
    you might want to add features like:
    - LRU eviction when memory is full
    - Persistence to disk
    - Compression for large values
    """
    
    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.ttl: Dict[str, float] = {}  # key -> expiration timestamp
        self.lock = threading.RLock()  # Thread-safe operations
        
        # Start cleanup thread for expired keys
        self.cleanup_thread = threading.Thread(target=self._cleanup_expired, daemon=True)
        self.cleanup_thread.start()
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value by key, returning None if expired or missing."""
        with self.lock:
            # Check if key exists and hasn't expired
            if key not in self.data:
                return None
            
            if key in self.ttl and time.time() > self.ttl[key]:
                # Key has expired, remove it
                del self.data[key]
                del self.ttl[key]
                return None
            
            return self.data[key]
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set a key-value pair with optional TTL."""
        with self.lock:
            self.data[key] = value
            
            if ttl_seconds is not None:
                self.ttl[key] = time.time() + ttl_seconds
            elif key in self.ttl:
                # Remove TTL if it was previously set
                del self.ttl[key]
    
    def delete(self, key: str) -> bool:
        """Delete a key, returning True if it existed."""
        with self.lock:
            existed = key in self.data
            if existed:
                del self.data[key]
                if key in self.ttl:
                    del self.ttl[key]
            return existed
    
    def clear(self) -> None:
        """Clear all cached data."""
        with self.lock:
            self.data.clear()
            self.ttl.clear()
    
    def size(self) -> int:
        """Return the number of stored keys."""
        with self.lock:
            return len(self.data)
    
    def stats(self) -> Dict[str, Any]:
        """Return cache statistics."""
        with self.lock:
            current_time = time.time()
            expired_count = sum(1 for exp_time in self.ttl.values() 
                              if current_time > exp_time)
            
            return {
                "total_keys": len(self.data),
                "keys_with_ttl": len(self.ttl),
                "expired_keys": expired_count,
                "memory_usage_estimate": sum(len(str(k)) + len(str(v)) 
                                           for k, v in self.data.items())
            }
    
    def _cleanup_expired(self) -> None:
        """Background thread to clean up expired keys."""
        while True:
            try:
                current_time = time.time()
                with self.lock:
                    expired_keys = [key for key, exp_time in self.ttl.items() 
                                  if current_time > exp_time]
                    
                    for key in expired_keys:
                        if key in self.data:
                            del self.data[key]
                        del self.ttl[key]
                
                # Clean up every 60 seconds
                time.sleep(60)
            except Exception as e:
                print(f"Error in cleanup thread: {e}")
                time.sleep(60)


class CacheRequestHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler for cache operations.
    
    Supports:
    - GET /cache/<key> - Retrieve a value
    - POST /cache/<key> - Store a value  
    - DELETE /cache/<key> - Delete a value
    - GET /stats - Get cache statistics
    - GET /health - Health check
    """
    
    def do_GET(self):
        """Handle GET requests (retrieve data or stats)."""
        parsed_url = urlparse(self.path)
        path_parts = parsed_url.path.strip('/').split('/')
        
        if path_parts[0] == 'cache' and len(path_parts) == 2:
            # GET /cache/<key>
            key = path_parts[1]
            value = self.server.storage.get(key)
            
            if value is not None:
                self._send_json_response(200, {"key": key, "value": value})
            else:
                self._send_json_response(404, {"error": "Key not found"})
        
        elif parsed_url.path == '/stats':
            # GET /stats
            stats = self.server.storage.stats()
            stats['node_id'] = self.server.node_id
            stats['port'] = self.server.server_port
            self._send_json_response(200, stats)
        
        elif parsed_url.path == '/health':
            # GET /health
            self._send_json_response(200, {
                "status": "healthy", 
                "node_id": self.server.node_id,
                "uptime": time.time() - self.server.start_time
            })
        
        else:
            self._send_json_response(404, {"error": "Not found"})
    
    def do_POST(self):
        """Handle POST requests (store data)."""
        parsed_url = urlparse(self.path)
        path_parts = parsed_url.path.strip('/').split('/')
        
        if path_parts[0] == 'cache' and len(path_parts) == 2:
            # POST /cache/<key>
            key = path_parts[1]
            
            try:
                # Read request body
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8')
                data = json.loads(body)
                
                value = data.get('value')
                ttl = data.get('ttl')  # Optional TTL in seconds
                
                if value is None:
                    self._send_json_response(400, {"error": "Missing 'value' in request body"})
                    return
                
                self.server.storage.set(key, value, ttl)
                self._send_json_response(200, {
                    "message": "Key stored successfully",
                    "key": key,
                    "value": value,
                    "ttl": ttl
                })
                
            except json.JSONDecodeError:
                self._send_json_response(400, {"error": "Invalid JSON in request body"})
            except Exception as e:
                self._send_json_response(500, {"error": str(e)})
        
        else:
            self._send_json_response(404, {"error": "Not found"})
    
    def do_DELETE(self):
        """Handle DELETE requests (remove data)."""
        parsed_url = urlparse(self.path)
        path_parts = parsed_url.path.strip('/').split('/')
        
        if path_parts[0] == 'cache' and len(path_parts) == 2:
            # DELETE /cache/<key>
            key = path_parts[1]
            existed = self.server.storage.delete(key)
            
            if existed:
                self._send_json_response(200, {"message": "Key deleted successfully"})
            else:
                self._send_json_response(404, {"error": "Key not found"})
        
        else:
            self._send_json_response(404, {"error": "Not found"})
    
    def _send_json_response(self, status_code: int, data: Dict[str, Any]) -> None:
        """Send a JSON response with the given status code and data."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')  # For web clients
        self.end_headers()
        
        response_json = json.dumps(data, indent=2)
        self.wfile.write(response_json.encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to add node_id to log messages."""
        print(f"[{self.server.node_id}] {format % args}")


class CacheNode:
    """
    A distributed cache node.
    
    Each node runs an HTTP server that can store and retrieve key-value pairs.
    Multiple nodes work together using consistent hashing to form a distributed cache.
    """
    
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.port = port
        self.storage = CacheStorage()
        self.start_time = time.time()
        self.server = None
    
    def start(self) -> None:
        """Start the cache node HTTP server."""
        try:
            self.server = HTTPServer(('localhost', self.port), CacheRequestHandler)
            
            # Add custom attributes to the server
            self.server.storage = self.storage
            self.server.node_id = self.node_id
            self.server.server_port = self.port
            self.server.start_time = self.start_time
            
            print(f"Cache node '{self.node_id}' starting on port {self.port}")
            print(f"Health check: http://localhost:{self.port}/health")
            print(f"Stats: http://localhost:{self.port}/stats")
            
            self.server.serve_forever()
            
        except KeyboardInterrupt:
            print(f"\nShutting down cache node '{self.node_id}'")
            self.stop()
        except Exception as e:
            print(f"Error starting cache node '{self.node_id}': {e}")
    
    def stop(self) -> None:
        """Stop the cache node server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            print(f"Cache node '{self.node_id}' stopped")


def main():
    """
    Start a single cache node.
    
    Usage: python cache_node.py [node_id] [port]
    """
    import sys
    
    # Parse command line arguments
    if len(sys.argv) >= 3:
        node_id = sys.argv[1]
        port = int(sys.argv[2])
    elif len(sys.argv) == 2:
        node_id = sys.argv[1]
        port = 8001  # Default port
    else:
        node_id = "node1"
        port = 8001
    
    # Start the cache node
    node = CacheNode(node_id, port)
    node.start()


if __name__ == "__main__":
    main()