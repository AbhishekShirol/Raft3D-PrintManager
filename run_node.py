import argparse
import sys
import threading
import time
from api.app import create_app
from raft.node import RaftNode

def parse_args():
    parser = argparse.ArgumentParser(description='Run a Raft3D node')
    parser.add_argument('--node-id', type=str, required=True, help='Unique node ID')
    parser.add_argument('--raft-port', type=int, required=True, help='Port for Raft communication')
    parser.add_argument('--api-port', type=int, required=True, help='Port for HTTP API')
    parser.add_argument('--cluster', type=str, required=True, help='Comma-separated list of node addresses (hostname:port)')
    return parser.parse_args()

def main():
    args = parse_args()
    cluster_nodes = args.cluster.split(',')
    
    # Initialize Raft node
    raft_node = RaftNode(node_id=args.node_id, port=args.raft_port, cluster_nodes=cluster_nodes)
    
    # Start Raft node in a separate thread
    raft_thread = threading.Thread(target=raft_node.start)
    raft_thread.daemon = True
    raft_thread.start()
    
    # Wait for Raft node to initialize
    time.sleep(1)
    
    # Create and run Flask app with reference to the Raft node
    app = create_app(raft_node)
    app.run(host='0.0.0.0', port=args.api_port)

if __name__ == '__main__':
    main()