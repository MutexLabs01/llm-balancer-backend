package node

import (
	"sync"
	"time"
	"llm-balancer/internal/models"
)

type TaskTracker struct {
	tasks       map[string]*models.Task
	mu          sync.RWMutex
	maxTaskAge  time.Duration
	cleanupInterval time.Duration
}

func NewTaskTracker() *TaskTracker {
	tracker := &TaskTracker{
		tasks:           make(map[string]*models.Task),
		maxTaskAge:      24 * time.Hour,
		cleanupInterval: 1 * time.Hour,  
	}
	
	go tracker.cleanupLoop()
	
	return tracker
}

func (tt *TaskTracker) AddTask(task *models.Task) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	
	tt.tasks[task.ID] = task
}

func (tt *TaskTracker) UpdateTask(taskID string, updater func(*models.Task)) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	
	if task, exists := tt.tasks[taskID]; exists {
		updater(task)
	}
}

func (tt *TaskTracker) GetTask(taskID string) (*models.Task, bool) {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	
	task, exists := tt.tasks[taskID]
	return task, exists
}

func (tt *TaskTracker) GetTasksByStatus(status models.TaskStatus) []*models.Task {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	
	var tasks []*models.Task
	for _, task := range tt.tasks {
		if task.Status == status {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (tt *TaskTracker) GetFailedTasks() []*models.Task {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	
	var failedTasks []*models.Task
	now := time.Now()
	
	for _, task := range tt.tasks {
		if task.Status == models.TaskStatusFailed {
			failedTasks = append(failedTasks, task)
		} else if task.Status == models.TaskStatusRunning {
			if task.StartedAt != nil && now.Sub(*task.StartedAt) > 5*time.Minute {
				failedTasks = append(failedTasks, task)
			}
		}
	}
	
	return failedTasks
}

func (tt *TaskTracker) GetOrphanedTasks() []*models.Task {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	
	var orphanedTasks []*models.Task
	now := time.Now()
	
	for _, task := range tt.tasks {
		if task.Status == models.TaskStatusPending {
			if now.Sub(task.CreatedAt) > 10*time.Minute {
				orphanedTasks = append(orphanedTasks, task)
			}
		}
	}
	
	return orphanedTasks
}

func (tt *TaskTracker) RemoveTask(taskID string) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	
	delete(tt.tasks, taskID)
}

func (tt *TaskTracker) GetStats() map[string]interface{} {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	
	stats := map[string]interface{}{
		"total_tasks": len(tt.tasks),
		"by_status":   make(map[models.TaskStatus]int),
		"by_age":      make(map[string]int),
	}
	
	now := time.Now()
	
	for _, task := range tt.tasks {
		if count, exists := stats["by_status"].(map[models.TaskStatus]int); exists {
			count[task.Status]++
		}
		
		age := now.Sub(task.CreatedAt)
		ageGroup := "recent"
		if age > 1*time.Hour {
			ageGroup = "old"
		} else if age > 10*time.Minute {
			ageGroup = "medium"
		}
		
		if ageCount, exists := stats["by_age"].(map[string]int); exists {
			ageCount[ageGroup]++
		}
	}
	
	return stats
}

func (tt *TaskTracker) cleanupLoop() {
	ticker := time.NewTicker(tt.cleanupInterval)
	defer ticker.Stop()
	
	for range ticker.C {
		tt.cleanup()
	}
}

func (tt *TaskTracker) cleanup() {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	
	now := time.Now()
	
	for taskID, task := range tt.tasks {
		if now.Sub(task.CreatedAt) > tt.maxTaskAge {
			delete(tt.tasks, taskID)
		}
	}
}

func (tt *TaskTracker) MarkTaskForRedistribution(taskID string) {
	tt.UpdateTask(taskID, func(task *models.Task) {
		task.Status = models.TaskStatusPending
		task.StartedAt = nil
		task.CompletedAt = nil
		task.Result = nil
		task.Error = ""
	})
}

func (tt *TaskTracker) GetTasksForRedistribution() []*models.Task {
	failedTasks := tt.GetFailedTasks()
	orphanedTasks := tt.GetOrphanedTasks()
	
	taskMap := make(map[string]*models.Task)
	
	for _, task := range failedTasks {
		taskMap[task.ID] = task
	}
	
	for _, task := range orphanedTasks {
		taskMap[task.ID] = task
	}
	
	var tasks []*models.Task
	for _, task := range taskMap {
		tasks = append(tasks, task)
	}
	
	return tasks
}

func (tt *TaskTracker) GetAllTasks() []*models.Task {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	
	var tasks []*models.Task
	for _, task := range tt.tasks {
		tasks = append(tasks, task)
	}
	return tasks
}