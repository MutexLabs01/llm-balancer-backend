#!/bin/bash

# Script to start all LLM Balancer components

echo "Starting LLM Balancer components..."

# Start Ollama if not running
if ! pgrep -x "ollama" > /dev/null; then
    echo "Starting Ollama..."
    ollama serve > ollama.log 2>&1 &
    sleep 5
else
    echo "Ollama is already running"
fi

# Start Gateway
echo "Starting Gateway..."
./bin/gateway config-gateway.yaml > gateway.log 2>&1 &
GATEWAY_PID=$!
sleep 3

# Start Node
echo "Starting Node..."
./bin/node config-test.yaml > node.log 2>&1 &
NODE_PID=$!
sleep 3

# Start Client for testing
echo "Starting Test Client..."
./bin/client > client.log 2>&1 &

echo "All components started!"
echo "Gateway PID: $GATEWAY_PID"
echo "Node PID: $NODE_PID"
echo ""
echo "Logs are available in:"
echo "- ollama.log"
echo "- gateway.log" 
echo "- node.log"
echo "- client.log"
echo ""
echo "Press Ctrl+C to stop all components"

# Wait for Ctrl+C
trap "echo 'Stopping all components...'; kill $GATEWAY_PID $NODE_PID; exit" INT

# Keep script running
while true; do
    sleep 1
done