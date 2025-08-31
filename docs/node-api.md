# LLM Balancer Node API Documentation

## Base URL
`http://<node-address>:<port>`

## API Version
All API endpoints are versioned under `/api/v1`

---

## Health & Info Endpoints

### GET /health
Get node health status.

**Response:**
```json
{
  "status": "healthy",
  "node_id": "node-001",
  "time": "2025-09-02T10:00:00Z",
  "checks": {
    "queue_healthy": true,
    "capacity_healthy": true,
    "error_rate_acceptable": true
  },
  "metrics": {
    "queue_size": 5,
    "active_tasks": 3,
    "avg_processing_time": "2.5s"
  }
}
```

### GET /
### GET /api/v1/info
Get node information.

**Response:**
```json
{
  "name": "LLM Balancer Node",
  "version": "1.0.0",
  "node_id": "node-001",
  "address": "192.168.1.100:8081",
  "uptime": "24h30m",
  "api_version": "v1",
  "features": [
    "task_processing",
    "work_stealing",
    "gossip_protocol",
    "auto_scaling",
    "health_monitoring"
  ]
}
```

---

## Task Management

### POST /api/v1/tasks
Submit a new task for processing.

**Request Body:**
```json
{
  "payload": "base64_encoded_data",
  "parameters": {
    "model": "qwen2.5",
    "temperature": 0.7,
    "max_tokens": 500
  },
  "priority": 5
}
```

**Response:**
```json
{
  "task_id": "task-1693737600000000000",
  "status": "queued",
  "message": "Task added to queue",
  "estimated_tokens": 125,
  "queue_position": 3
}
```

### GET /api/v1/tasks/:taskId
Get task details by ID.

**Response:**
```json
{
  "id": "task-1693737600000000000",
  "status": "completed",
  "payload": "base64_encoded_data",
  "parameters": {...},
  "priority": 5,
  "created_at": "2025-09-02T10:00:00Z",
  "started_at": "2025-09-02T10:00:05Z",
  "completed_at": "2025-09-02T10:00:15Z",
  "node_id": "node-001",
  "result": {
    "response": "base64_encoded_response",
    "tokens_used": 120,
    "processing_time": "10s",
    "metadata": {...}
  }
}
```

### GET /api/v1/tasks
List tasks with optional filtering.

**Query Parameters:**
- `status`: Filter by task status
- `limit`: Maximum number of tasks (default: 100)
- `offset`: Pagination offset (default: 0)

**Response:**
```json
{
  "tasks": [...],
  "count": 25,
  "limit": "100",
  "offset": "0"
}
```

### DELETE /api/v1/tasks/:taskId
Cancel a pending or running task.

**Response:**
```json
{
  "message": "Task cancelled successfully"
}
```

### GET /api/v1/tasks/failed
Get failed tasks that can be redistributed.

**Response:**
```json
{
  "tasks": [...],
  "count": 3
}
```

### POST /api/v1/tasks/steal
Steal a task from this node (work stealing).

**Response:**
```json
{
  "id": "task-1693737600000000000",
  "status": "pending",
  ...
}
```

### POST /api/v1/tasks/retry/:taskId
Retry a failed task.

**Response:**
```json
{
  "message": "Task queued for retry"
}
```

---

## Node Status & Capacity

### GET /api/v1/status
Get comprehensive node status.

**Response:**
```json
{
  "node_status": {
    "id": "node-001",
    "address": "192.168.1.100:8081",
    "health": true,
    "last_heartbeat": "2025-09-02T10:00:00Z",
    "active_tasks": 3,
    "queue_length": 5
  },
  "capacity": {
    "max_requests_per_minute": 100,
    "max_tokens_per_minute": 10000,
    "max_concurrent_tasks": 10,
    "max_queue_size": 50,
    "current_requests_per_minute": 25,
    "current_tokens_per_minute": 2500,
    "current_concurrent_tasks": 3
  },
  "queue_size": 5,
  "worker_pool_size": 4,
  "uptime": "24h30m"
}
```

### GET /api/v1/capacity
Get node capacity details.

**Response:**
```json
{
  "max_requests_per_minute": 100,
  "max_tokens_per_minute": 10000,
  "max_concurrent_tasks": 10,
  "max_queue_size": 50,
  "current_requests_per_minute": 25,
  "current_tokens_per_minute": 2500,
  "current_concurrent_tasks": 3
}
```

### PUT /api/v1/capacity
Update node capacity limits.

**Request Body:**
```json
{
  "max_requests_per_minute": 120,
  "max_tokens_per_minute": 12000,
  "max_concurrent_tasks": 12,
  "max_queue_size": 60
}
```

**Response:**
```json
{
  "message": "Capacity updated",
  "capacity": {...}
}
```

---

## Performance & Metrics

### GET /api/v1/metrics
Get comprehensive node metrics.

**Response:**
```json
{
  "processing_metrics": {
    "avg_processing_time": "2.5s",
    "error_counts": {
      "timeout": 2,
      "ollama_error": 1
    },
    "success_counts": {
      "completed": 95
    },
    "queue_size": 5,
    "active_tasks": 3,
    "last_updated": "2025-09-02T10:00:00Z"
  },
  "queue_stats": {...},
  "capacity": {...},
  "node_status": {...},
  "timestamp": "2025-09-02T10:00:00Z"
}
```

### GET /api/v1/performance
Get performance analytics.

**Response:**
```json
{
  "avg_processing_time": "2.5s",
  "error_rate": 0.05,
  "throughput": 15.5,
  "queue_efficiency": 0.85,
  "worker_utilization": 0.75,
  "memory_usage": {
    "allocated": "512MB",
    "total": "1GB",
    "percent": 50.0
  },
  "cpu_usage": {
    "percent": 35.0,
    "cores": 4
  }
}
```

---

## Queue Management

### GET /api/v1/queue/stats
Get queue statistics.

**Response:**
```json
{
  "size": 5,
  "max_size": 50,
  "utilization": 0.1,
  "push_count": 100,
  "pop_count": 95,
  "type": "work_stealing"
}
```

### GET /api/v1/queue/size
Get current queue size.

**Response:**
```json
{
  "queue_size": 5,
  "max_size": 50,
  "utilization": 0.1
}
```

### POST /api/v1/queue/clear
Clear all tasks from the queue.

**Response:**
```json
{
  "message": "Queue cleared",
  "tasks_cleared": 5
}
```

### GET /api/v1/queue/priority
Get tasks by priority level.

**Query Parameters:**
- `priority`: Priority level (0-10)

**Response:**
```json
{
  "tasks": [...],
  "count": 3,
  "priority": "5"
}
```

---

## Worker Management

### GET /api/v1/workers
Get all workers information.

**Response:**
```json
{
  "workers": [
    {
      "id": "worker-0",
      "status": "active",
      "type": "work-stealing"
    },
    {
      "id": "worker-1",
      "status": "active",
      "type": "work-stealing"
    }
  ],
  "count": 4
}
```

### GET /api/v1/workers/:workerId
Get specific worker information.

**Response:**
```json
{
  "id": "worker-0",
  "status": "active",
  "current_task": "task-1693737600000000000",
  "tasks_processed": 25,
  "avg_process_time": "2.5s"
}
```

### PUT /api/v1/workers/:workerId/pause
Pause a specific worker.

**Response:**
```json
{
  "message": "Worker paused",
  "worker_id": "worker-0"
}
```

### PUT /api/v1/workers/:workerId/resume
Resume a paused worker.

**Response:**
```json
{
  "message": "Worker resumed",
  "worker_id": "worker-0"
}
```

---

## Cluster & Gossip Protocol

### GET /api/v1/cluster/peers
Get cluster peers.

**Response:**
```json
{
  "peers": {
    "node-002": {
      "id": "node-002",
      "address": "192.168.1.101:8081",
      "last_seen": "2025-09-02T09:59:55Z",
      "status": "alive",
      "load": 0.6,
      "latency": "5ms"
    }
  },
  "count": 3
}
```

### POST /api/v1/cluster/peers
Add a new peer to the cluster.

**Request Body:**
```json
{
  "peer_id": "node-003",
  "address": "192.168.1.102:8081"
}
```

**Response:**
```json
{
  "message": "Peer added successfully",
  "peer_id": "node-003"
}
```

### DELETE /api/v1/cluster/peers/:peerId
Remove a peer from the cluster.

**Response:**
```json
{
  "message": "Peer removed successfully",
  "peer_id": "node-003"
}
```

### POST /api/v1/cluster/gossip
Handle gossip messages from other nodes.

**Request Body:**
```json
{
  "type": "heartbeat",
  "source_node": "node-002",
  "timestamp": "2025-09-02T10:00:00Z",
  "data": {...}
}
```

**Response:**
```json
{
  "status": "received"
}
```

### GET /api/v1/cluster/topology
Get cluster topology information.

**Response:**
```json
{
  "node_id": "node-001",
  "peers": {...},
  "peer_count": 3,
  "cluster_status": "connected"
}
```

---

## Configuration

### GET /api/v1/config
Get current node configuration.

**Response:**
```json
{
  "node_id": "node-001",
  "address": "192.168.1.100:8081",
  "max_queue_size": 50,
  "worker_pool_size": 4,
  "heartbeat_interval": "30s",
  "use_work_stealing": true
}
```

### PUT /api/v1/config
Update node configuration.

**Request Body:**
```json
{
  "max_queue_size": 60,
  "worker_pool_size": 6
}
```

**Response:**
```json
{
  "message": "Configuration updated",
  "config": {...}
}
```

### POST /api/v1/config/reload
Reload configuration from file.

**Response:**
```json
{
  "message": "Configuration reloaded"
}
```

---

## Administrative

### POST /api/v1/admin/shutdown
Gracefully shutdown the node.

**Response:**
```json
{
  "message": "Shutdown initiated"
}
```

### POST /api/v1/admin/restart
Restart the node service.

**Response:**
```json
{
  "message": "Restart initiated"
}
```

### GET /api/v1/admin/logs
Get node logs.

**Query Parameters:**
- `level`: Log level filter (debug, info, warn, error)
- `limit`: Maximum number of log entries

**Response:**
```json
{
  "logs": [
    {
      "timestamp": "2025-09-02T10:00:00Z",
      "level": "info",
      "message": "Task completed successfully",
      "node_id": "node-001"
    }
  ],
  "count": 100,
  "limit": "100"
}
```

### POST /api/v1/admin/reset
Reset node metrics and counters.

**Response:**
```json
{
  "message": "Node metrics reset"
}
```

---

## Error Responses

All endpoints may return these error responses:

### 400 Bad Request
```json
{
  "error": "Invalid request body"
}
```

### 404 Not Found
```json
{
  "error": "Task not found"
}
```

### 500 Internal Server Error
```json
{
  "error": "Internal server error"
}
```

### 503 Service Unavailable
```json
{
  "error": "Node queue is full"
}
```

---

## Task Status Values

- `pending`: Task is queued and waiting to be processed
- `running`: Task is currently being processed
- `completed`: Task completed successfully
- `failed`: Task processing failed

## Node Status Values

- `alive`: Node is healthy and responsive
- `suspected`: Node may be experiencing issues
- `dead`: Node is unresponsive or failed
