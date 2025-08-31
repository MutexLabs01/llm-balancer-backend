# LLM Balancer Node - API Structure Overview

## Complete API Endpoint Structure

### Root Endpoints
```
GET  /                    - Node information
GET  /health              - Health check
```

### Task Management (`/api/v1/tasks`)
```
POST   /api/v1/tasks                - Submit new task
GET    /api/v1/tasks                - List all tasks (with filtering)
GET    /api/v1/tasks/:taskId        - Get specific task
DELETE /api/v1/tasks/:taskId        - Cancel task
GET    /api/v1/tasks/failed         - Get failed tasks
POST   /api/v1/tasks/steal          - Steal task (work stealing)
POST   /api/v1/tasks/retry/:taskId  - Retry failed task
```

### Node Status & Health (`/api/v1/`)
```
GET /api/v1/status       - Comprehensive node status
GET /api/v1/health       - Health check
GET /api/v1/info         - Node information
```

### Capacity & Performance (`/api/v1/`)
```
GET /api/v1/capacity     - Get capacity details
PUT /api/v1/capacity     - Update capacity limits
GET /api/v1/metrics      - Comprehensive metrics
GET /api/v1/performance  - Performance analytics
```

### Queue Management (`/api/v1/queue`)
```
GET  /api/v1/queue/stats    - Queue statistics
GET  /api/v1/queue/size     - Current queue size
POST /api/v1/queue/clear    - Clear queue
GET  /api/v1/queue/priority - Get tasks by priority
```

### Worker Management (`/api/v1/workers`)
```
GET /api/v1/workers              - List all workers
GET /api/v1/workers/:workerId    - Get specific worker
PUT /api/v1/workers/:workerId/pause  - Pause worker
PUT /api/v1/workers/:workerId/resume - Resume worker
```

### Cluster & Gossip (`/api/v1/cluster`)
```
GET    /api/v1/cluster/peers        - Get cluster peers
POST   /api/v1/cluster/peers        - Add peer
DELETE /api/v1/cluster/peers/:peerId - Remove peer
POST   /api/v1/cluster/gossip       - Handle gossip messages
GET    /api/v1/cluster/topology     - Get cluster topology
```

### Configuration (`/api/v1/config`)
```
GET  /api/v1/config        - Get configuration
PUT  /api/v1/config        - Update configuration
POST /api/v1/config/reload - Reload configuration
```

### Administrative (`/api/v1/admin`)
```
POST /api/v1/admin/shutdown - Graceful shutdown
POST /api/v1/admin/restart  - Restart service
GET  /api/v1/admin/logs     - Get logs
POST /api/v1/admin/reset    - Reset metrics
```

