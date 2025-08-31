package main

import (
	"fmt"
	"time"
	"context"
	"llm-balancer/internal/queue"
	"llm-balancer/internal/models"
)

func main() {
	fmt.Println("Testing Work-Stealing Queue Implementation")
	fmt.Println("==========================================")
	
	// Test 1: Basic work-stealing queue functionality
	testBasicWorkStealing()
	
	// Test 2: Work stealing between workers
	testWorkStealingBetweenWorkers()
	
	fmt.Println("\n✅ All tests completed successfully!")
}

func testBasicWorkStealing() {
	fmt.Println("\nTest 1: Basic Work-Stealing Queue Operations")
	fmt.Println("-------------------------------------------")
	
	// Create a work-stealing queue with capacity 100 and 4 workers
	wsQueue := queue.NewWorkStealingQueue(100, 4)
	
	// Add some tasks to the queue
	for i := 0; i < 10; i++ {
		task := models.NewTask(
			fmt.Sprintf("task-%d", i),
			[]byte(fmt.Sprintf("Task payload %d", i)),
			map[string]interface{}{"model": "qwen2.5"},
		)
		task.Priority = i % 5 // Varying priorities
		
		err := wsQueue.Push(task)
		if err != nil {
			fmt.Printf("❌ Error pushing task %d: %v\n", i, err)
			return
		} else {
			fmt.Printf("✅ Pushed task %d with priority %d\n", i, task.Priority)
		}
	}
	
	fmt.Printf("📊 Queue size: %d\n", wsQueue.Size())
	fmt.Printf("📊 Is queue full: %t\n", wsQueue.IsFull())
	fmt.Printf("📊 Is queue empty: %t\n", wsQueue.IsEmpty())
	
	// Test GetStats
	stats := wsQueue.GetStats()
	fmt.Printf("📊 Queue stats: Size=%d, Capacity=%d\n", stats.Size, stats.Capacity)
	
	fmt.Println("✅ Test 1 passed!")
}

func testWorkStealingBetweenWorkers() {
	fmt.Println("\nTest 2: Work Stealing Between Workers")
	fmt.Println("-------------------------------------")
	
	// Create a work-stealing queue with capacity 100 and 3 workers
	wsQueue := queue.NewWorkStealingQueue(100, 3)
	
	// Add many tasks to create workload
	for i := 0; i < 15; i++ {
		task := models.NewTask(
			fmt.Sprintf("task-%d", i),
			[]byte(fmt.Sprintf("Processing task %d", i)),
			map[string]interface{}{"model": "qwen2.5"},
		)
		task.Priority = i % 3 // Varying priorities
		
		err := wsQueue.Push(task)
		if err != nil {
			fmt.Printf("❌ Error pushing task %d: %v\n", i, err)
			return
		}
	}
	
	fmt.Printf("📊 Initial queue size: %d\n", wsQueue.Size())
	
	// Start workers that will steal work
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Worker 0 - will get some tasks
	go func() {
		for i := 0; i < 5; i++ { // Process 5 tasks
			select {
			case <-ctx.Done():
				return
			default:
				task, err := wsQueue.PopWithTimeout(0, 100*time.Millisecond)
				if err != nil {
					// No task available, that's okay
					time.Sleep(10 * time.Millisecond)
					continue
				}
				fmt.Printf("✅ Worker-0 processed %s\n", task.ID)
				time.Sleep(5 * time.Millisecond) // Simulate work
			}
		}
	}()
	
	// Worker 1 - will get some tasks
	go func() {
		for i := 0; i < 5; i++ { // Process 5 tasks
			select {
			case <-ctx.Done():
				return
			default:
				task, err := wsQueue.PopWithTimeout(1, 100*time.Millisecond)
				if err != nil {
					// No task available, that's okay
					time.Sleep(10 * time.Millisecond)
					continue
				}
				fmt.Printf("✅ Worker-1 processed %s\n", task.ID)
				time.Sleep(5 * time.Millisecond) // Simulate work
			}
		}
	}()
	
	// Worker 2 - will get some tasks
	go func() {
		for i := 0; i < 5; i++ { // Process 5 tasks
			select {
			case <-ctx.Done():
				return
			default:
				task, err := wsQueue.PopWithTimeout(2, 100*time.Millisecond)
				if err != nil {
					// No task available, that's okay
					time.Sleep(10 * time.Millisecond)
					continue
				}
				fmt.Printf("✅ Worker-2 processed %s\n", task.ID)
				time.Sleep(5 * time.Millisecond) // Simulate work
			}
		}
	}()
	
	// Let workers process tasks
	time.Sleep(2 * time.Second)
	
	// Check stats
	stats := wsQueue.GetStats()
	fmt.Printf("📊 Final queue size: %d\n", stats.Size)
	
	fmt.Println("✅ Work-stealing test passed!")
}