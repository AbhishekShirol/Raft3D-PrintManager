#!/bin/bash

# Create logs directory
mkdir -p logs

# Start three nodes
python run_node.py --node-id node1 --raft-port 5001 --api-port 8001 --cluster localhost:5001,localhost:5002,localhost:5003 > logs/node1.log 2>&1 &
echo "Started node1 on port 8001"

python run_node.py --node-id node2 --raft-port 5002 --api-port 8002 --cluster localhost:5001,localhost:5002,localhost:5003 > logs/node2.log 2>&1 &
echo "Started node2 on port 8002"

python run_node.py --node-id node3 --raft-port 5003 --api-port 8003 --cluster localhost:5001,localhost:5002,localhost:5003 > logs/node3.log 2>&1 &
echo "Started node3 on port 8003"

echo "Raft cluster started. Use API on ports 8001, 8002, 8003"
echo "Check logs in logs/ directory"