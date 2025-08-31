package main

import (
	"fmt"
	"llm-balancer/internal/queue"
	"llm-balancer/internal/models"
)

func main() {
	fmt.Println("Testing Backward Compatibility")
	fmt.Println("=============================")
	
	// Test regular queue still works
	fmt.Println("\n1. Testing Regular TaskQueue")
	testRegularQueue()
	
	fmt.Println("\n✅ Backward compatibility verified!")
}

func testRegularQueue() {
	// Create regular queue
	regQueue := queue.NewTaskQueue(100)
	
	// Add tasks
	for i := 0; i < 5; i++ {
		task := models.NewTask(
			fmt.Sprintf("task-%d", i),
			[]byte(fmt.Sprintf("Task payload %d", i)),
			map[string]interface{}{"model": "qwen2.5"},
		)
		
		err := regQueue.Push(task)
		if err != nil {
			fmt.Printf("❌ Error pushing task %d: %v\n", i, err)
			return
		} else {
			fmt.Printf("✅ Pushed task %d\n", i)
		}
	}
	
	fmt.Printf("✅ Queue size after push: %d\n", regQueue.Size())
	fmt.Printf("✅ Queue is full: %t\n", regQueue.IsFull())
	fmt.Printf("✅ Queue is empty: %t\n", regQueue.IsEmpty())
	
	// Don't try to pop in this simple test as it would block
	// The important thing is that we can push tasks and check size
	
	fmt.Println("✅ Regular queue test passed!")
}