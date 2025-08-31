package queue

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
	"llm-balancer/internal/models"
)

// WorkStealingQueue implements a work-stealing queue for better load distribution
type WorkStealingQueue struct {
	localQueues []*TaskHeap
	globalQueue *TaskHeap
	capacity    int
	numWorkers  int
	mu          sync.RWMutex
	notify      []chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	
	// Metrics
	metrics           map[string]interface{}
	lastOperationTime time.Time
	operationCounts   map[string]int
	errorCounts       map[string]int
	avgWaitTime       time.Duration
	peakSize          int
	stealCount        int
}

// TaskHeap implements heap.Interface for priority queue
type TaskHeap []*models.Task

func (h TaskHeap) Len() int { return len(h) }

func (h TaskHeap) Less(i, j int) bool {
	// Higher priority first, then by creation time (FIFO for same priority)
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}

func (h TaskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TaskHeap) Push(x interface{}) {
	item := x.(*models.Task)
	*h = append(*h, item)
}

func (h *TaskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func NewWorkStealingQueue(capacity, numWorkers int) *WorkStealingQueue {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create local queues for each worker
	localQueues := make([]*TaskHeap, numWorkers)
	notify := make([]chan struct{}, numWorkers)
	
	for i := 0; i < numWorkers; i++ {
		localQueues[i] = &TaskHeap{}
		heap.Init(localQueues[i])
		notify[i] = make(chan struct{}, 1)
	}
	
	globalQueue := &TaskHeap{}
	heap.Init(globalQueue)
	
	queue := &WorkStealingQueue{
		localQueues: localQueues,
		globalQueue: globalQueue,
		capacity:    capacity,
		numWorkers:  numWorkers,
		notify:      notify,
		ctx:         ctx,
		cancel:      cancel,
		metrics:     make(map[string]interface{}),
		operationCounts: make(map[string]int),
		errorCounts: make(map[string]int),
	}
	
	return queue
}

func (wsq *WorkStealingQueue) Push(task *models.Task) error {
	wsq.mu.Lock()
	defer wsq.mu.Unlock()
	
	// Validate task
	if task == nil {
		wsq.recordError("push", "nil_task")
		return ErrInvalidTask
	}
	
	if len(task.Payload) == 0 {
		wsq.recordError("push", "empty_payload")
		return ErrInvalidTask
	}
	
	// Check capacity
	totalSize := wsq.getTotalSizeLocked()
	if totalSize >= wsq.capacity {
		wsq.recordError("push", "queue_full")
		return ErrQueueFull
	}
	
	workerID := int(time.Now().UnixNano()) % wsq.numWorkers
	heap.Push(wsq.localQueues[workerID], task)
	
	wsq.recordOperation("push")
	wsq.updatePeakSizeLocked(totalSize + 1)
	wsq.updateLastOperationTime()
	
	select {
	case wsq.notify[workerID] <- struct{}{}:
	default:
	}
	
	return nil
}

func (wsq *WorkStealingQueue) Pop(workerID int) (*models.Task, error) {
	return wsq.PopWithTimeout(workerID, 0)
}

// PopWithTimeout removes and returns a task for a specific worker with timeout
func (wsq *WorkStealingQueue) PopWithTimeout(workerID int, timeout time.Duration) (*models.Task, error) {
	startTime := time.Now()
	
	for {
		// Try to get task from local queue first
		wsq.mu.Lock()
		if wsq.localQueues[workerID].Len() > 0 {
			task := heap.Pop(wsq.localQueues[workerID]).(*models.Task)
			wsq.mu.Unlock()
			
			// Update metrics
			wsq.recordOperation("pop")
			wsq.updateWaitTime(time.Since(startTime))
			wsq.updateLastOperationTime()
			
			return task, nil
		}
		wsq.mu.Unlock()
		
		// Try to steal from other workers
		task := wsq.stealWork(workerID)
		if task != nil {
			// Update metrics
			wsq.recordOperation("steal")
			wsq.updateWaitTime(time.Since(startTime))
			wsq.updateLastOperationTime()
			wsq.stealCount++
			
			return task, nil
		}
		
		// Try global queue
		wsq.mu.Lock()
		if wsq.globalQueue.Len() > 0 {
			task := heap.Pop(wsq.globalQueue).(*models.Task)
			wsq.mu.Unlock()
			
			// Update metrics
			wsq.recordOperation("pop")
			wsq.updateWaitTime(time.Since(startTime))
			wsq.updateLastOperationTime()
			
			return task, nil
		}
		wsq.mu.Unlock()
		
		// Wait for notification or timeout
		if timeout == 0 {
			select {
			case <-wsq.notify[workerID]:
				continue
			case <-wsq.ctx.Done():
				wsq.recordError("pop", "context_cancelled")
				return nil, ErrQueueClosed
			}
		} else {
			select {
			case <-wsq.notify[workerID]:
				continue
			case <-time.After(timeout):
				wsq.recordError("pop", "timeout")
				return nil, ErrTimeout
			case <-wsq.ctx.Done():
				wsq.recordError("pop", "context_cancelled")
				return nil, ErrQueueClosed
			}
		}
	}
}

// stealWork attempts to steal work from other workers
func (wsq *WorkStealingQueue) stealWork(workerID int) *models.Task {
	// Try to steal from other workers in round-robin fashion
	for i := 1; i < wsq.numWorkers; i++ {
		victimID := (workerID + i) % wsq.numWorkers
		
		wsq.mu.Lock()
		if wsq.localQueues[victimID].Len() > 0 {
			// Steal from the bottom of the victim's queue (FIFO for work stealing)
			victimQueue := wsq.localQueues[victimID]
			task := (*victimQueue)[0]
			*victimQueue = (*victimQueue)[1:]
			heap.Init(victimQueue) // Re-heapify after removal
			wsq.mu.Unlock()
			
			return task
		}
		wsq.mu.Unlock()
	}
	
	return nil
}

// getTotalSize returns the total number of items across all queues (locked version)
func (wsq *WorkStealingQueue) getTotalSizeLocked() int {
	total := wsq.globalQueue.Len()
	for _, localQueue := range wsq.localQueues {
		total += localQueue.Len()
	}
	
	return total
}

func (wsq *WorkStealingQueue) getTotalSize() int {
	wsq.mu.RLock()
	defer wsq.mu.RUnlock()
	
	return wsq.getTotalSizeLocked()
}

func (wsq *WorkStealingQueue) Size() int {
	return wsq.getTotalSize()
}

func (wsq *WorkStealingQueue) IsEmpty() bool {
	return wsq.Size() == 0
}

func (wsq *WorkStealingQueue) IsFull() bool {
	return wsq.Size() >= wsq.capacity
}

func (wsq *WorkStealingQueue) updatePeakSizeLocked(currentSize int) {
	if currentSize > wsq.peakSize {
		wsq.peakSize = currentSize
	}
}

func (wsq *WorkStealingQueue) Close() {
	wsq.cancel()
	for _, notifyChan := range wsq.notify {
		close(notifyChan)
	}
}

func (wsq *WorkStealingQueue) GetStats() QueueStats {
	wsq.mu.RLock()
	defer wsq.mu.RUnlock()
	
	stats := QueueStats{
		Size:     wsq.getTotalSize(),
		Capacity: wsq.capacity,
		Status:   make(map[models.TaskStatus]int),
		Metrics:  wsq.getMetrics(),
	}
	
	for _, localQueue := range wsq.localQueues {
		for _, task := range *localQueue {
			stats.Status[task.Status]++
		}
	}
	
	for _, task := range *wsq.globalQueue {
		stats.Status[task.Status]++
	}
	
	return stats
}

func (wsq *WorkStealingQueue) recordOperation(operation string) {
	wsq.operationCounts[operation]++
}

func (wsq *WorkStealingQueue) recordError(operation, errorType string) {
	key := fmt.Sprintf("%s_%s", operation, errorType)
	wsq.errorCounts[key]++
}

func (wsq *WorkStealingQueue) updatePeakSize() {
	currentSize := wsq.getTotalSize()
	if currentSize > wsq.peakSize {
		wsq.peakSize = currentSize
	}
}

func (wsq *WorkStealingQueue) updateLastOperationTime() {
	wsq.lastOperationTime = time.Now()
}

func (wsq *WorkStealingQueue) updateWaitTime(waitTime time.Duration) {
	if wsq.avgWaitTime == 0 {
		wsq.avgWaitTime = waitTime
	} else {
		wsq.avgWaitTime = (wsq.avgWaitTime + waitTime) / 2
	}
}

func (wsq *WorkStealingQueue) getMetrics() map[string]interface{} {
	utilization := 0.0
	if wsq.capacity > 0 {
		utilization = float64(wsq.getTotalSize()) / float64(wsq.capacity) * 100
	}
	
	return map[string]interface{}{
		"utilization_percentage": utilization,
		"peak_size":             wsq.peakSize,
		"avg_wait_time":         wsq.avgWaitTime,
		"last_operation_time":   wsq.lastOperationTime,
		"operation_counts":      wsq.operationCounts,
		"error_counts":          wsq.errorCounts,
		"is_empty":              wsq.getTotalSize() == 0,
		"is_full":               wsq.getTotalSize() >= wsq.capacity,
		"steal_count":           wsq.stealCount,
	}
}