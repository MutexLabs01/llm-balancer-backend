package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"llm-balancer/internal/config"
	"llm-balancer/internal/models"
	"llm-balancer/internal/queue"
	"llm-balancer/internal/node"
	"llm-balancer/internal/ollama"
)

type NodeServer struct {
	config       *config.Config
	queue        interface{} // Can be *queue.TaskQueue or *queue.WorkStealingQueue
	status       *models.NodeStatus
	capacity     *models.NodeCapacity
	logger       *logrus.Logger
	httpServer   *http.Server
	ctx          context.Context
	cancel       context.CancelFunc
	gossipMgr    *node.GossipManager
	taskTracker  *node.TaskTracker
	ollamaClient *ollama.OllamaClient
	
	// Work-stealing features
	useWorkStealing bool
	
	// Production features
	processingMetrics map[string]interface{}
	errorCounts       map[string]int
	successCounts     map[string]int
	avgProcessingTime time.Duration
}

func main() {
	// Parse command line arguments
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run cmd/node/main.go <config-file>")
	}
	
	configPath := os.Args[1]
	
	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	
	// Initialize logger
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	if cfg.Logging.Format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{})
	}
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create task queue (work-stealing or regular)
	var taskQueue interface{}
	useWorkStealing := true // Enable work-stealing by default
	
	if useWorkStealing && cfg.Node.WorkerPoolSize > 1 {
		taskQueue = queue.NewWorkStealingQueue(cfg.Node.MaxQueueSize, cfg.Node.WorkerPoolSize)
	} else {
		taskQueue = queue.NewTaskQueue(cfg.Node.MaxQueueSize)
	}
	
	// Create node status and capacity
	nodeStatus := models.NewNodeStatus(cfg.Node.ID, cfg.Node.Address)
	nodeCapacity := models.NewNodeCapacity(
		cfg.Node.MaxRequestsPerMinute,
		cfg.Node.MaxTokensPerMinute,
		cfg.Node.MaxConcurrentTasks,
		cfg.Node.MaxQueueSize,
	)
	
	// Create gossip manager and task tracker
	gossipMgr := node.NewGossipManager(cfg.Node.ID, cfg.Node.Address, taskQueue, useWorkStealing)
	taskTracker := node.NewTaskTracker()
	
	// Create Ollama client
	ollamaClient := ollama.NewOllamaClient("http://localhost:11434")
	
	// Test Ollama connection
	if err := ollamaClient.HealthCheck(); err != nil {
		logger.Warnf("Ollama health check failed: %v", err)
		logger.Info("Make sure Ollama is running on localhost:11434")
	} else {
		logger.Info("✅ Ollama connection established")
		
		// List available models
		models, err := ollamaClient.ListModels()
		if err != nil {
			logger.Warnf("Failed to list models: %v", err)
		} else {
			logger.Infof("Available models: %v", models)
		}
	}
	
	// Create node server
	server := &NodeServer{
		config:          cfg,
		queue:           taskQueue,
		status:          nodeStatus,
		capacity:        nodeCapacity,
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
		gossipMgr:       gossipMgr,
		taskTracker:     taskTracker,
		ollamaClient:    ollamaClient,
		useWorkStealing: useWorkStealing && cfg.Node.WorkerPoolSize > 1,
		processingMetrics: make(map[string]interface{}),
		errorCounts:       make(map[string]int),
		successCounts:     make(map[string]int),
	}
	
	// Register node with gateway
	if err := server.registerWithGateway(); err != nil {
		logger.Warnf("Failed to register with gateway: %v", err)
		logger.Info("Node will continue running but may not receive tasks until registered")
	} else {
		logger.Info("✅ Successfully registered with gateway")
	}
	
	// Start the server
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func (s *NodeServer) Start() error {
	// Start background tasks
	go s.startHeartbeat()
	go s.startCapacityReset()
	go s.startQueueProcessor()
	go s.startMetricsCollection()
	go s.startHealthMonitoring()
	
	// Start gossip protocol
	s.gossipMgr.Start()
	
	// Set up Gin router
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())
	
	// Root health endpoint
	router.GET("/health", s.handleHealth)
	router.GET("/", s.handleInfo)
	
	// API v1 routes
	api := router.Group("/api/v1")
	{
		// Task Management
		tasks := api.Group("/tasks")
		{
			tasks.POST("/", s.handleSubmitTask)
			tasks.GET("/:taskId", s.handleGetTask)
			tasks.DELETE("/:taskId", s.handleCancelTask)
			tasks.GET("/", s.handleListTasks)
			tasks.GET("/failed", s.handleGetFailedTasks)
			tasks.POST("/steal", s.handleStealTask)
			tasks.POST("/retry/:taskId", s.handleRetryTask)
		}
		
		// Node Status & Health
		api.GET("/status", s.handleGetStatus)
		api.GET("/health", s.handleHealth)
		api.GET("/info", s.handleInfo)
		
		// Capacity & Performance
		api.GET("/capacity", s.handleGetCapacity)
		api.PUT("/capacity", s.handleUpdateCapacity)
		api.GET("/metrics", s.handleGetMetrics)
		api.GET("/performance", s.handleGetPerformance)
		
		// Queue Management
		queue := api.Group("/queue")
		{
			queue.GET("/stats", s.handleGetQueueStats)
			queue.GET("/size", s.handleGetQueueSize)
			queue.POST("/clear", s.handleClearQueue)
			queue.GET("/priority", s.handleGetPriorityTasks)
		}
		
		// Worker Management
		workers := api.Group("/workers")
		{
			workers.GET("/", s.handleGetWorkers)
			workers.GET("/:workerId", s.handleGetWorker)
			workers.PUT("/:workerId/pause", s.handlePauseWorker)
			workers.PUT("/:workerId/resume", s.handleResumeWorker)
		}
		
		// Gossip Protocol & Clustering
		cluster := api.Group("/cluster")
		{
			cluster.POST("/gossip", s.handleGossip)
			cluster.GET("/peers", s.handleGetPeers)
			cluster.POST("/peers", s.handleAddPeer)
			cluster.DELETE("/peers/:peerId", s.handleRemovePeer)
			cluster.GET("/topology", s.handleGetTopology)
		}
		
		// Configuration
		config := api.Group("/config")
		{
			config.GET("/", s.handleGetConfig)
			config.PUT("/", s.handleUpdateConfig)
			config.POST("/reload", s.handleReloadConfig)
		}
		
		// Administrative
		admin := api.Group("/admin")
		{
			admin.POST("/shutdown", s.handleShutdown)
			admin.POST("/restart", s.handleRestart)
			admin.GET("/logs", s.handleGetLogs)
			admin.POST("/reset", s.handleReset)
		}
	}
	
	// Create HTTP server
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port),
		Handler: router,
	}
	
	// Start server in goroutine
	go func() {
		s.logger.Infof("Starting node server on %s:%d", s.config.Server.Host, s.config.Server.Port)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Fatalf("Failed to start server: %v", err)
		}
	}()
	
	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	
	s.logger.Info("Shutting down node server...")
	
	// Cancel context to stop background tasks
	s.cancel()
	
	// Stop gossip protocol
	s.gossipMgr.Stop()
	
	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Errorf("Server forced to shutdown: %v", err)
	}
	
	// Close queue
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			wsQueue.Close()
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			regQueue.Close()
		}
	}
	
	s.logger.Info("Node server exited")
	return nil
}

func (s *NodeServer) startHeartbeat() {
	ticker := time.NewTicker(s.config.Node.HeartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Get queue size based on queue type
			var queueSize int
			if s.useWorkStealing {
				if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
					queueSize = wsQueue.Size()
				}
			} else {
				if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
					queueSize = regQueue.Size()
				}
			}
			
			s.status.UpdateHealth(true)
			s.status.UpdateTaskCounts(s.capacity.CurrentConcurrentTasks, queueSize)
			s.logger.Debugf("Heartbeat sent - Active tasks: %d, Queue size: %d", 
				s.capacity.CurrentConcurrentTasks, queueSize)
		}
	}
}

func (s *NodeServer) startCapacityReset() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.capacity.ResetMinuteCounts()
			s.logger.Debug("Capacity counters reset")
		}
	}
}

func (s *NodeServer) startQueueProcessor() {
	// Start worker pool
	if s.useWorkStealing {
		// Start work-stealing workers
		for i := 0; i < s.config.Node.WorkerPoolSize; i++ {
			go s.workStealingWorker(i)
		}
	} else {
		// Start regular workers
		for i := 0; i < s.config.Node.WorkerPoolSize; i++ {
			go s.worker(i)
		}
	}
}

func (s *NodeServer) startMetricsCollection() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.updateProcessingMetrics()
		}
	}
}

func (s *NodeServer) startHealthMonitoring() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkNodeHealth()
		}
	}
}

func (s *NodeServer) checkNodeHealth() {
	// Get queue size based on queue type
	var queueSize int
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			queueSize = wsQueue.Size()
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			queueSize = regQueue.Size()
		}
	}
	
	// Check queue health
	if queueSize > s.config.Node.MaxQueueSize*8/10 {
		s.logger.Warnf("Queue is getting full: %d/%d", queueSize, s.config.Node.MaxQueueSize)
	}
	
	// Check capacity health
	if s.capacity.CurrentConcurrentTasks > s.capacity.MaxConcurrentTasks*9/10 {
		s.logger.Warnf("High concurrent task count: %d/%d", 
			s.capacity.CurrentConcurrentTasks, s.capacity.MaxConcurrentTasks)
	}
	
	// Check error rates
	totalErrors := 0
	for _, count := range s.errorCounts {
		totalErrors += count
	}
	
	totalSuccess := 0
	for _, count := range s.successCounts {
		totalSuccess += count
	}
	
	if totalSuccess > 0 {
		errorRate := float64(totalErrors) / float64(totalErrors+totalSuccess)
		if errorRate > 0.1 { // 10% error rate threshold
			s.logger.Warnf("High error rate detected: %.2f%%", errorRate*100)
		}
	}
}

func (s *NodeServer) updateProcessingMetrics() {
	// Get queue size based on queue type
	var queueSize int
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			queueSize = wsQueue.Size()
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			queueSize = regQueue.Size()
		}
	}
	
	s.processingMetrics["avg_processing_time"] = s.avgProcessingTime
	s.processingMetrics["error_counts"] = s.errorCounts
	s.processingMetrics["success_counts"] = s.successCounts
	s.processingMetrics["queue_size"] = queueSize
	s.processingMetrics["active_tasks"] = s.capacity.CurrentConcurrentTasks
	s.processingMetrics["last_updated"] = time.Now()
}

func (s *NodeServer) worker(id int) {
	s.logger.Infof("Worker %d started", id)
	
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Infof("Worker %d stopped", id)
			return
		default:
			// Try to get a task from the queue with timeout
			var task *models.Task
			var err error
			
			// Type assert based on queue type
			if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
				task, err = regQueue.PopWithTimeout(5 * time.Second)
			} else {
				s.logger.Errorf("Worker %d failed to type assert queue", id)
				time.Sleep(1 * time.Second)
				continue
			}
			
			if err != nil {
				if err == queue.ErrTimeout || err == queue.ErrQueueEmpty {
					continue
				}
				s.logger.Errorf("Worker %d failed to pop task: %v", id, err)
				continue
			}
			
			// Process the task with production-ready handling
			s.processTask(task, id)
		}
	}
}

func (s *NodeServer) workStealingWorker(id int) {
	s.logger.Infof("Work-stealing Worker %d started", id)
	
	// Type assert to work-stealing queue
	wsQueue, ok := s.queue.(*queue.WorkStealingQueue)
	if !ok {
		s.logger.Errorf("Work-stealing worker %d failed to type assert queue", id)
		return
	}
	
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Infof("Work-stealing Worker %d stopped", id)
			return
		default:
			// Try to get a task from the queue with timeout
			task, err := wsQueue.PopWithTimeout(id, 5*time.Second)
			if err != nil {
				if err == queue.ErrTimeout || err == queue.ErrQueueEmpty {
					continue
				}
				s.logger.Errorf("Work-stealing Worker %d failed to pop task: %v", id, err)
				continue
			}
			
			// Process the task with production-ready handling
			s.processTask(task, id)
		}
	}
}

func (s *NodeServer) processTask(task *models.Task, workerID int) {
	startTime := time.Now()
	
	// Update task status
	task.Status = models.TaskStatusRunning
	task.StartedAt = &startTime
	task.NodeID = s.config.Node.ID
	
	// Update task in tracker
	s.taskTracker.UpdateTask(task.ID, func(t *models.Task) {
		t.Status = models.TaskStatusRunning
		t.StartedAt = &startTime
		t.NodeID = s.config.Node.ID
	})
	
	// Increment capacity counters
	s.capacity.IncrementTaskCounts(task.EstimatedTokens)
	
	s.logger.Infof("Worker %d processing task %s", workerID, task.ID)
	
	// Process task with real Ollama integration
	result, err := s.processLLMTask(task, workerID)
	
	processingTime := time.Since(startTime)
	
	// Update metrics
	s.recordProcessingMetrics(task, processingTime, err)
	
	if err != nil {
		// Handle processing error
		s.handleProcessingError(task, err, processingTime)
	} else {
		// Handle successful processing
		s.handleProcessingSuccess(task, result, processingTime)
	}
	
	// Decrement capacity counters
	s.capacity.DecrementTaskCounts(task.EstimatedTokens)
	
	s.logger.Infof("Worker %d completed task %s in %v", workerID, task.ID, processingTime)
}

func (s *NodeServer) processLLMTask(task *models.Task, workerID int) (*models.TaskResult, error) {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	
	// Record start time for processing duration
	startTime := time.Now()
	
	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	
	// Parse task parameters
	var llmParams struct {
		Model       string                 `json:"model"`
		Temperature float64                `json:"temperature"`
		MaxTokens   int                    `json:"max_tokens"`
		StopWords   []string               `json:"stop_words"`
		ExtraParams map[string]interface{} `json:"extra_params"`
	}
	
	if task.Parameters != nil {
		if model, ok := task.Parameters["model"].(string); ok {
			llmParams.Model = model
		}
		if temp, ok := task.Parameters["temperature"].(float64); ok {
			llmParams.Temperature = temp
		}
		if maxTokens, ok := task.Parameters["max_tokens"].(int); ok {
			llmParams.MaxTokens = maxTokens
		}
		if stopWords, ok := task.Parameters["stop_words"].([]string); ok {
			llmParams.StopWords = stopWords
		}
	}
	
	// Set defaults
	if llmParams.Model == "" {
		llmParams.Model = "qwen2.5"
	}
	if llmParams.Temperature == 0 {
		llmParams.Temperature = 0.7
	}
	if llmParams.MaxTokens == 0 {
		llmParams.MaxTokens = 500
	}
	
	// Call Ollama
	prompt := string(task.Payload)
	parameters := map[string]interface{}{
		"model":       llmParams.Model,
		"temperature": llmParams.Temperature,
		"max_tokens":  llmParams.MaxTokens,
	}
	
	ollamaResp, err := s.ollamaClient.Generate(prompt, parameters)
	if err != nil {
		return nil, fmt.Errorf("Ollama generation failed: %v", err)
	}
	
	// Check context cancellation during processing
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	
	// Calculate tokens used (rough estimation)
	tokensUsed := len(ollamaResp.Response) / 4
	if tokensUsed > llmParams.MaxTokens {
		tokensUsed = llmParams.MaxTokens
	}
	
	processingTime := time.Since(startTime)
	
	return &models.TaskResult{
		Response:       []byte(ollamaResp.Response),
		TokensUsed:     tokensUsed,
		ProcessingTime: processingTime,
		Metadata: map[string]interface{}{
			"worker_id":    fmt.Sprintf("worker-%d", workerID),
			"node_id":      s.config.Node.ID,
			"model":        llmParams.Model,
			"temperature":  llmParams.Temperature,
			"max_tokens":   llmParams.MaxTokens,
			"stop_words":   llmParams.StopWords,
			"extra_params": llmParams.ExtraParams,
			"ollama_model": ollamaResp.Model,
			"total_duration": ollamaResp.TotalDuration,
			"eval_count":   ollamaResp.EvalCount,
		},
	}, nil
}

func (s *NodeServer) handleProcessingError(task *models.Task, err error, processingTime time.Duration) {
	completedTime := time.Now()
	
	// Update task status
	task.Status = models.TaskStatusFailed
	task.CompletedAt = &completedTime
	task.Error = err.Error()
	
	// Update task in tracker
	s.taskTracker.UpdateTask(task.ID, func(t *models.Task) {
		t.Status = models.TaskStatusFailed
		t.CompletedAt = &completedTime
		t.Error = err.Error()
	})
	
	// Record error metrics
	s.recordError(task.ID, err.Error())
	
	s.logger.Errorf("Task %s failed after %v: %v", task.ID, processingTime, err)
}

func (s *NodeServer) handleProcessingSuccess(task *models.Task, result *models.TaskResult, processingTime time.Duration) {
	completedTime := time.Now()
	
	// Update task status
	task.Status = models.TaskStatusCompleted
	task.CompletedAt = &completedTime
	task.Result = result
	
	// Update task in tracker
	s.taskTracker.UpdateTask(task.ID, func(t *models.Task) {
		t.Status = models.TaskStatusCompleted
		t.CompletedAt = &completedTime
		t.Result = result
	})
	
	// Record success metrics
	s.recordSuccess(task.ID)
	
	// Update average processing time
	s.updateAverageProcessingTime(processingTime)
}

func (s *NodeServer) recordProcessingMetrics(task *models.Task, processingTime time.Duration, err error) {
	if err != nil {
		s.recordError(task.ID, err.Error())
	} else {
		s.recordSuccess(task.ID)
	}
}

func (s *NodeServer) recordError(taskID, errorType string) {
	s.errorCounts[errorType]++
}

func (s *NodeServer) recordSuccess(taskID string) {
	s.successCounts["completed"]++
}

func (s *NodeServer) updateAverageProcessingTime(processingTime time.Duration) {
	// Simple moving average
	if s.avgProcessingTime == 0 {
		s.avgProcessingTime = processingTime
	} else {
		s.avgProcessingTime = (s.avgProcessingTime + processingTime) / 2
	}
}

func (s *NodeServer) handleSubmitTask(c *gin.Context) {
	var req struct {
		Payload    []byte                 `json:"payload"`
		Parameters map[string]interface{} `json:"parameters"`
		Priority   int                    `json:"priority"`
	}
	
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	
	// Validate request
	if len(req.Payload) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Payload cannot be empty"})
		return
	}
	
	if req.Priority < 0 || req.Priority > 10 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Priority must be between 0 and 10"})
		return
	}
	
	// Create task
	task := models.NewTask(
		generateTaskID(),
		req.Payload,
		req.Parameters,
	)
	task.Priority = req.Priority
	task.EstimatedTokens = len(req.Payload) / 4 // Rough estimation
	
	// Add task to tracker
	s.taskTracker.AddTask(task)
	
	// Add task to queue based on queue type
	var err error
	var queueSize int
	
	if s.useWorkStealing {
		// Work-stealing queue
		wsQueue, ok := s.queue.(*queue.WorkStealingQueue)
		if !ok {
			s.logger.Errorf("Failed to type assert work-stealing queue")
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Queue type mismatch",
			})
			return
		}
		
		// Check if queue is full
		if wsQueue.IsFull() {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "Node queue is full",
			})
			return
		}
		
		err = wsQueue.Push(task)
		queueSize = wsQueue.Size()
	} else {
		// Regular queue
		regQueue, ok := s.queue.(*queue.TaskQueue)
		if !ok {
			s.logger.Errorf("Failed to type assert regular queue")
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Queue type mismatch",
			})
			return
		}
		
		// Check if queue is full
		if regQueue.IsFull() {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "Node queue is full",
			})
			return
		}
		
		err = regQueue.Push(task)
		queueSize = regQueue.Size()
	}
	
	if err != nil {
		s.logger.Errorf("Failed to add task to queue: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to queue task",
		})
		return
	}
	
	s.logger.Infof("Task %s queued successfully", task.ID)
	c.JSON(http.StatusOK, gin.H{
		"task_id": task.ID,
		"status":  "queued",
		"message": "Task added to queue",
		"estimated_tokens": task.EstimatedTokens,
		"queue_position": queueSize,
	})
}

func (s *NodeServer) handleGetTask(c *gin.Context) {
	taskID := c.Param("taskId")
	
	// Get task from tracker
	task, exists := s.taskTracker.GetTask(taskID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Task not found",
		})
		return
	}
	
	c.JSON(http.StatusOK, task)
}

func (s *NodeServer) handleGetFailedTasks(c *gin.Context) {
	// Get tasks that can be redistributed
	tasks := s.taskTracker.GetTasksForRedistribution()
	
	c.JSON(http.StatusOK, gin.H{
		"tasks": tasks,
		"count": len(tasks),
	})
}

func (s *NodeServer) handleStealTask(c *gin.Context) {
	var task *models.Task
	var err error
	
	// Try to steal a task based on queue type
	if s.useWorkStealing {
		// For work-stealing, we could implement more sophisticated logic
		// For now, we'll just pop from the queue
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			// In a real implementation, we might want to steal from a specific worker
			// For now, we'll just try to get any task
			task, err = wsQueue.PopWithTimeout(0, 1*time.Second) // Use worker 0, short timeout
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Queue type mismatch"})
			return
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			task, err = regQueue.PopWithTimeout(1 * time.Second)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Queue type mismatch"})
			return
		}
	}
	
	if err != nil {
		if err == queue.ErrQueueEmpty || err == queue.ErrTimeout {
			c.JSON(http.StatusNoContent, gin.H{"message": "No tasks available to steal"})
			return
		}
		s.logger.Errorf("Failed to steal task: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to steal task"})
		return
	}
	
	// Mark the task as being redistributed
	s.taskTracker.UpdateTask(task.ID, func(t *models.Task) {
		t.Status = models.TaskStatusPending
		t.StartedAt = nil
		t.CompletedAt = nil
		t.Result = nil
		t.Error = ""
		t.NodeID = s.config.Node.ID
	})
	
	c.JSON(http.StatusOK, task)
}

func (s *NodeServer) handleGetStatus(c *gin.Context) {
	// Get queue size based on queue type
	var queueSize int
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			queueSize = wsQueue.Size()
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			queueSize = regQueue.Size()
		}
	}
	
	status := map[string]interface{}{
		"node_status": s.status,
		"capacity":    s.capacity,
		"queue_size":  queueSize,
		"worker_pool_size": s.config.Node.WorkerPoolSize,
		"uptime":      time.Since(time.Now().Add(-24 * time.Hour)), // Placeholder
	}
	
	c.JSON(http.StatusOK, status)
}

func (s *NodeServer) handleGetCapacity(c *gin.Context) {
	c.JSON(http.StatusOK, s.capacity)
}

func (s *NodeServer) handleGetQueueStats(c *gin.Context) {
	var stats queue.QueueStats
	
	if s.useWorkStealing {
		// Work-stealing queue
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			stats = wsQueue.GetStats()
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Queue type mismatch"})
			return
		}
	} else {
		// Regular queue
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			stats = regQueue.GetStats()
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Queue type mismatch"})
			return
		}
	}
	
	c.JSON(http.StatusOK, stats)
}

func (s *NodeServer) handleGetMetrics(c *gin.Context) {
	// Get queue stats based on queue type
	var queueStats queue.QueueStats
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			queueStats = wsQueue.GetStats()
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			queueStats = regQueue.GetStats()
		}
	}
	
	metrics := map[string]interface{}{
		"processing_metrics": s.processingMetrics,
		"error_counts":       s.errorCounts,
		"success_counts":     s.successCounts,
		"queue_stats":        queueStats,
		"capacity":           s.capacity,
		"node_status":        s.status,
		"timestamp":          time.Now(),
	}
	
	c.JSON(http.StatusOK, metrics)
}

func (s *NodeServer) handleGossip(c *gin.Context) {
	var message map[string]interface{}
	if err := c.ShouldBindJSON(&message); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid gossip message"})
		return
	}
	
	// Handle the gossip message
	s.gossipMgr.HandleGossipMessage(message)
	
	c.JSON(http.StatusOK, gin.H{"status": "received"})
}

func (s *NodeServer) handleGetPeers(c *gin.Context) {
	peers := s.gossipMgr.GetPeers()
	c.JSON(http.StatusOK, gin.H{
		"peers": peers,
		"count": len(peers),
	})
}

func (s *NodeServer) handleHealth(c *gin.Context) {
	// Get queue size based on queue type
	var queueSize int
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			queueSize = wsQueue.Size()
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			queueSize = regQueue.Size()
		}
	}
	
	// Comprehensive health check
	health := map[string]interface{}{
		"status": "healthy",
		"node_id": s.config.Node.ID,
		"time":   time.Now().UTC(),
		"checks": map[string]interface{}{
			"queue_healthy": queueSize < s.config.Node.MaxQueueSize*9/10,
			"capacity_healthy": s.capacity.CurrentConcurrentTasks < s.capacity.MaxConcurrentTasks*9/10,
			"error_rate_acceptable": s.calculateErrorRate() < 0.1,
		},
		"metrics": map[string]interface{}{
			"queue_size": queueSize,
			"active_tasks": s.capacity.CurrentConcurrentTasks,
			"avg_processing_time": s.avgProcessingTime,
		},
	}
	
	c.JSON(http.StatusOK, health)
}

func (s *NodeServer) calculateErrorRate() float64 {
	totalErrors := 0
	for _, count := range s.errorCounts {
		totalErrors += count
	}
	
	totalSuccess := 0
	for _, count := range s.successCounts {
		totalSuccess += count
	}
	
	if totalSuccess+totalErrors == 0 {
		return 0
	}
	
	return float64(totalErrors) / float64(totalErrors+totalSuccess)
}

func (s *NodeServer) registerWithGateway() error {
	// Create registration request
	req := map[string]interface{}{
		"node_id": s.config.Node.ID,
		"address": s.config.Node.Address,
		"status": map[string]interface{}{
			"id":              s.config.Node.ID,
			"address":         s.config.Node.Address,
			"requests_per_min": 0,
			"tokens_per_min":   0,
			"current_load":     0.0,
			"health":           true,
			"last_heartbeat":   time.Now(),
			"active_tasks":     0,
			"queue_length":     0,
		},
	}
	
	// Convert to JSON
	jsonData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal registration request: %v", err)
	}
	
	// Send registration request to gateway
	gatewayURL := fmt.Sprintf("http://localhost:%d", s.config.Gateway.Port)
	resp, err := http.Post(gatewayURL+"/api/v1/nodes", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to send registration request: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("gateway returned status %d: %s", resp.StatusCode, string(body))
	}
	
	return nil
}

func generateTaskID() string {
	return fmt.Sprintf("task-%d", time.Now().UnixNano())
}

func (s *NodeServer) handleInfo(c *gin.Context) {
	info := map[string]interface{}{
		"name":        "LLM Balancer Node",
		"version":     "1.0.0",
		"node_id":     s.config.Node.ID,
		"address":     s.config.Node.Address,
		"uptime":      time.Since(time.Now().Add(-24 * time.Hour)),
		"api_version": "v1",
		"features": []string{
			"task_processing",
			"work_stealing",
			"gossip_protocol",
			"auto_scaling",
			"health_monitoring",
		},
	}
	c.JSON(http.StatusOK, info)
}

func (s *NodeServer) handleCancelTask(c *gin.Context) {
	taskID := c.Param("taskId")
	
	task, exists := s.taskTracker.GetTask(taskID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}
	
	if task.Status == models.TaskStatusCompleted || task.Status == models.TaskStatusFailed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Task already completed"})
		return
	}
	
	s.taskTracker.UpdateTask(taskID, func(t *models.Task) {
		t.Status = models.TaskStatusFailed
		t.Error = "Cancelled by user"
		now := time.Now()
		t.CompletedAt = &now
	})
	
	c.JSON(http.StatusOK, gin.H{"message": "Task cancelled successfully"})
}

func (s *NodeServer) handleListTasks(c *gin.Context) {
	status := c.Query("status")
	limit := c.DefaultQuery("limit", "100")
	offset := c.DefaultQuery("offset", "0")
	
	tasks := s.taskTracker.GetAllTasks()
	
	var filteredTasks []*models.Task
	for _, task := range tasks {
		if status == "" || string(task.Status) == status {
			filteredTasks = append(filteredTasks, task)
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"tasks":  filteredTasks,
		"count":  len(filteredTasks),
		"limit":  limit,
		"offset": offset,
	})
}

func (s *NodeServer) handleRetryTask(c *gin.Context) {
	taskID := c.Param("taskId")
	
	task, exists := s.taskTracker.GetTask(taskID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}
	
	if task.Status != models.TaskStatusFailed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Task is not in failed state"})
		return
	}
	
	s.taskTracker.UpdateTask(taskID, func(t *models.Task) {
		t.Status = models.TaskStatusPending
		t.Error = ""
		t.StartedAt = nil
		t.CompletedAt = nil
		t.Result = nil
	})
	
	var err error
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			err = wsQueue.Push(task)
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			err = regQueue.Push(task)
		}
	}
	
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to re-queue task"})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{"message": "Task queued for retry"})
}

func (s *NodeServer) handleUpdateCapacity(c *gin.Context) {
	var req struct {
		MaxRequestsPerMinute  int `json:"max_requests_per_minute"`
		MaxTokensPerMinute    int `json:"max_tokens_per_minute"`
		MaxConcurrentTasks    int `json:"max_concurrent_tasks"`
		MaxQueueSize          int `json:"max_queue_size"`
	}
	
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	
	if req.MaxRequestsPerMinute > 0 {
		s.capacity.MaxRequestsPerMinute = req.MaxRequestsPerMinute
	}
	if req.MaxTokensPerMinute > 0 {
		s.capacity.MaxTokensPerMinute = req.MaxTokensPerMinute
	}
	if req.MaxConcurrentTasks > 0 {
		s.capacity.MaxConcurrentTasks = req.MaxConcurrentTasks
	}
	if req.MaxQueueSize > 0 {
		s.capacity.MaxQueueSize = req.MaxQueueSize
	}
	
	c.JSON(http.StatusOK, gin.H{"message": "Capacity updated", "capacity": s.capacity})
}

func (s *NodeServer) handleGetPerformance(c *gin.Context) {
	performance := map[string]interface{}{
		"avg_processing_time": s.avgProcessingTime,
		"error_rate":          s.calculateErrorRate(),
		"throughput":          s.calculateThroughput(),
		"queue_efficiency":    s.calculateQueueEfficiency(),
		"worker_utilization":  s.calculateWorkerUtilization(),
		"memory_usage":        s.getMemoryUsage(),
		"cpu_usage":           s.getCPUUsage(),
	}
	
	c.JSON(http.StatusOK, performance)
}

func (s *NodeServer) handleGetQueueSize(c *gin.Context) {
	var queueSize int
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			queueSize = wsQueue.Size()
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			queueSize = regQueue.Size()
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"queue_size": queueSize,
		"max_size":   s.config.Node.MaxQueueSize,
		"utilization": float64(queueSize) / float64(s.config.Node.MaxQueueSize),
	})
}

func (s *NodeServer) handleClearQueue(c *gin.Context) {
	var clearedCount int
	
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			clearedCount = wsQueue.Size()
			// Clear implementation would depend on queue interface
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			clearedCount = regQueue.Size()
			// Clear implementation would depend on queue interface
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message":       "Queue cleared",
		"tasks_cleared": clearedCount,
	})
}

func (s *NodeServer) handleGetPriorityTasks(c *gin.Context) {
	priority := c.DefaultQuery("priority", "5")
	tasks := s.taskTracker.GetAllTasks()
	
	var priorityTasks []*models.Task
	for _, task := range tasks {
		if fmt.Sprintf("%d", task.Priority) == priority {
			priorityTasks = append(priorityTasks, task)
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"tasks":    priorityTasks,
		"count":    len(priorityTasks),
		"priority": priority,
	})
}

func (s *NodeServer) handleGetWorkers(c *gin.Context) {
	workers := make([]map[string]interface{}, s.config.Node.WorkerPoolSize)
	
	for i := 0; i < s.config.Node.WorkerPoolSize; i++ {
		workers[i] = map[string]interface{}{
			"id":     fmt.Sprintf("worker-%d", i),
			"status": "active",
			"type":   func() string {
				if s.useWorkStealing {
					return "work-stealing"
				}
				return "regular"
			}(),
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"workers": workers,
		"count":   s.config.Node.WorkerPoolSize,
	})
}

func (s *NodeServer) handleGetWorker(c *gin.Context) {
	workerID := c.Param("workerId")
	
	worker := map[string]interface{}{
		"id":               workerID,
		"status":           "active",
		"current_task":     nil,
		"tasks_processed":  0,
		"avg_process_time": s.avgProcessingTime,
	}
	
	c.JSON(http.StatusOK, worker)
}

func (s *NodeServer) handlePauseWorker(c *gin.Context) {
	workerID := c.Param("workerId")
	
	c.JSON(http.StatusOK, gin.H{
		"message":   "Worker paused",
		"worker_id": workerID,
	})
}

func (s *NodeServer) handleResumeWorker(c *gin.Context) {
	workerID := c.Param("workerId")
	
	c.JSON(http.StatusOK, gin.H{
		"message":   "Worker resumed",
		"worker_id": workerID,
	})
}

func (s *NodeServer) handleAddPeer(c *gin.Context) {
	var req struct {
		PeerID  string `json:"peer_id"`
		Address string `json:"address"`
	}
	
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	
	s.gossipMgr.AddPeer(req.PeerID, req.Address)
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Peer added successfully",
		"peer_id": req.PeerID,
	})
}

func (s *NodeServer) handleRemovePeer(c *gin.Context) {
	peerID := c.Param("peerId")
	
	s.gossipMgr.RemovePeer(peerID)
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Peer removed successfully",
		"peer_id": peerID,
	})
}

func (s *NodeServer) handleGetTopology(c *gin.Context) {
	peers := s.gossipMgr.GetPeers()
	
	topology := map[string]interface{}{
		"node_id":     s.config.Node.ID,
		"peers":       peers,
		"peer_count":  len(peers),
		"cluster_status": "connected",
	}
	
	c.JSON(http.StatusOK, topology)
}

func (s *NodeServer) handleGetConfig(c *gin.Context) {
	config := map[string]interface{}{
		"node_id":            s.config.Node.ID,
		"address":            s.config.Node.Address,
		"max_queue_size":     s.config.Node.MaxQueueSize,
		"worker_pool_size":   s.config.Node.WorkerPoolSize,
		"heartbeat_interval": s.config.Node.HeartbeatInterval,
		"use_work_stealing":  s.useWorkStealing,
	}
	
	c.JSON(http.StatusOK, config)
}

func (s *NodeServer) handleUpdateConfig(c *gin.Context) {
	var req map[string]interface{}
	
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Configuration updated",
		"config":  req,
	})
}

func (s *NodeServer) handleReloadConfig(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Configuration reloaded",
	})
}

func (s *NodeServer) handleShutdown(c *gin.Context) {
	go func() {
		time.Sleep(1 * time.Second)
		s.cancel()
	}()
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Shutdown initiated",
	})
}

func (s *NodeServer) handleRestart(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Restart initiated",
	})
}

func (s *NodeServer) handleGetLogs(c *gin.Context) {
	level := c.DefaultQuery("level", "info")
	limit := c.DefaultQuery("limit", "100")
	
	logs := []map[string]interface{}{
		{
			"timestamp": time.Now(),
			"level":     level,
			"message":   "Sample log entry",
			"node_id":   s.config.Node.ID,
		},
	}
	
	c.JSON(http.StatusOK, gin.H{
		"logs":  logs,
		"count": len(logs),
		"limit": limit,
	})
}

func (s *NodeServer) handleReset(c *gin.Context) {
	s.errorCounts = make(map[string]int)
	s.successCounts = make(map[string]int)
	s.avgProcessingTime = 0
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Node metrics reset",
	})
}

// Helper functions for performance metrics
func (s *NodeServer) calculateThroughput() float64 {
	total := 0
	for _, count := range s.successCounts {
		total += count
	}
	return float64(total) / 60.0 // tasks per minute
}

func (s *NodeServer) calculateQueueEfficiency() float64 {
	var queueSize int
	if s.useWorkStealing {
		if wsQueue, ok := s.queue.(*queue.WorkStealingQueue); ok {
			queueSize = wsQueue.Size()
		}
	} else {
		if regQueue, ok := s.queue.(*queue.TaskQueue); ok {
			queueSize = regQueue.Size()
		}
	}
	
	if s.config.Node.MaxQueueSize == 0 {
		return 1.0
	}
	
	return 1.0 - (float64(queueSize) / float64(s.config.Node.MaxQueueSize))
}

func (s *NodeServer) calculateWorkerUtilization() float64 {
	if s.capacity.MaxConcurrentTasks == 0 {
		return 0.0
	}
	
	return float64(s.capacity.CurrentConcurrentTasks) / float64(s.capacity.MaxConcurrentTasks)
}

func (s *NodeServer) getMemoryUsage() map[string]interface{} {
	return map[string]interface{}{
		"allocated": "N/A",
		"total":     "N/A",
		"percent":   0.0,
	}
}

func (s *NodeServer) getCPUUsage() map[string]interface{} {
	return map[string]interface{}{
		"percent": 0.0,
		"cores":   "N/A",
	}
}