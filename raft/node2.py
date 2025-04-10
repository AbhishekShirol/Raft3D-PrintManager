import json
import logging
import os
import random
import threading
import time
import uuid
from enum import Enum
import socket
import requests

from .state_machine2 import RaftStateMachine
from .storage import LogStorage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("RaftNode")

class NodeState(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

class LogEntry:
    def __init__(self, term, command, index=None):
        self.term = term
        self.command = command
        self.index = index  # Will be set when appended to log

    def to_dict(self):
        return {
            'term': self.term,
            'command': self.command,
            'index': self.index
        }

    @classmethod
    def from_dict(cls, data):
        entry = cls(data['term'], data['command'])
        entry.index = data['index']
        return entry

class RaftNode:
    def __init__(self, node_id, port, cluster_nodes):
        self.id = node_id
        self.address = f"localhost:{port}"  # For simplicity, using localhost
        self.port = port
        self.cluster_nodes = [node for node in cluster_nodes if node != self.address]
        
        # Raft state
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.election_timeout = self._get_random_election_timeout()
        self.last_heartbeat = time.time()
        
        # Log state
        self.log_storage = LogStorage(f"data/log/{self.id}")
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state
        self.next_index = {node: 1 for node in self.cluster_nodes}
        self.match_index = {node: 0 for node in self.cluster_nodes}
        
        # State machine
        self.state_machine = RaftStateMachine()
        
        # Lock for concurrent access
        self.lock = threading.RLock()
        
        # Server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', port))
        self.running = False
        
        # Flag to indicate if node has checked for existing leader
        self.checked_for_leader = False
        
        logger.info(f"Initialized node {self.id} on {self.address}")

    def start(self):
        """Start the Raft node."""
        self.running = True
        
        # Check for existing leader before starting election timeout
        self._check_for_existing_leader()
        
        # Start main loop in a separate thread
        main_thread = threading.Thread(target=self._main_loop)
        main_thread.daemon = True
        main_thread.start()
        
        # Start server to handle incoming RPCs
        self.server_socket.listen(5)
        
        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()
                client_handler = threading.Thread(target=self._handle_rpc, args=(client_socket,))
                client_handler.daemon = True
                client_handler.start()
            except Exception as e:
                if self.running:
                    logger.error(f"Error accepting connection: {e}")
        
        logger.info(f"Node {self.id} stopped")

    def stop(self):
        """Stop the Raft node."""
        self.running = False
        self.server_socket.close()

    def _check_for_existing_leader(self):
        """Check if there's an existing leader in the cluster before starting election timeout."""
        logger.info(f"Node {self.id} checking for existing leader in the cluster")
        
        # Send a heartbeat request to all nodes in the cluster
        for node in self.cluster_nodes:
            try:
                # Create a simple AppendEntries request with empty entries
                request = {
                    'type': 'append_entries',
                    'term': self.current_term,
                    'leader_id': '',  # We don't know the leader yet
                    'prev_log_index': 0,
                    'prev_log_term': 0,
                    'entries': [],
                    'leader_commit': 0
                }
                
                response = self._send_rpc(node, 'append_entries', request)
                
                # If we get a valid response with a term, update our term and reset heartbeat
                if response and 'term' in response:
                    if response['term'] > self.current_term:
                        with self.lock:
                            self._update_term(response['term'])
                            self.last_heartbeat = time.time()  # Reset election timeout
                            self.checked_for_leader = True
                            logger.info(f"Node {self.id} found existing leader or higher term, resetting timeout")
                            return True
            except Exception as e:
                logger.warning(f"Failed to check for leader with node {node}: {e}")
        
        logger.info(f"Node {self.id} did not find existing leader")
        self.checked_for_leader = True
        return False

    def _main_loop(self):
        """Main loop for the Raft node."""
        while self.running:
            with self.lock:
                current_time = time.time()
                
                if self.state == NodeState.FOLLOWER:
                    # Check if election timeout has passed
                    if current_time - self.last_heartbeat > self.election_timeout:
                        # Only become candidate if we've checked for an existing leader
                        if self.checked_for_leader:
                            logger.info(f"Node {self.id} election timeout, becoming candidate")
                            self._become_candidate()
                        else:
                            # Check for existing leader before starting election
                            self._check_for_existing_leader()
                
                elif self.state == NodeState.CANDIDATE:
                    # Check if election timeout has passed
                    if current_time - self.last_heartbeat > self.election_timeout:
                        logger.info(f"Node {self.id} election timeout as candidate, starting new election")
                        self._start_election()
                
                elif self.state == NodeState.LEADER:
                    # Send heartbeats (AppendEntries with no entries)
                    # Use a more consistent heartbeat interval (50ms)
                    if current_time - self.last_heartbeat > 0.05:  # Send heartbeat every 50ms
                        self._send_heartbeats()
                        self.last_heartbeat = current_time
                        # Log periodic heartbeat for debugging
                        if random.random() < 0.1:  # Only log occasionally to avoid flooding
                            logger.debug(f"Leader {self.id} sent heartbeat for term {self.current_term}")
                
                # Apply committed entries
                self._apply_committed_entries()
            
            # Sleep briefly to avoid burning CPU
            time.sleep(0.01)

    def _become_candidate(self):
        """Transition to candidate state and start an election."""
        self.state = NodeState.CANDIDATE
        self._start_election()

    def _start_election(self):
        """Start a leader election."""
        with self.lock:
            # Only start election if we're still a candidate
            if self.state != NodeState.CANDIDATE:
                return
                
            self.current_term += 1
            self.voted_for = self.id
            self.election_timeout = self._get_random_election_timeout()
            self.last_heartbeat = time.time()
            
            logger.info(f"Node {self.id} starting election for term {self.current_term}")
            
            # Create a thread-safe counter for votes
            votes_lock = threading.Lock()
            votes_received = 1  # Vote for self
            
            # Function to request vote from a single node
            def request_vote_from_node(node):
                nonlocal votes_received
                try:
                    response = self._send_request_vote_rpc(node)
                    
                    # Check if we received a higher term
                    if response.get('term', 0) > self.current_term:
                        with self.lock:
                            self._update_term(response['term'])
                        return
                    
                    # Count the vote if granted
                    if response.get('vote_granted', False):
                        with votes_lock:
                            votes_received += 1
                            
                            # Check if we have majority
                            if votes_received > (len(self.cluster_nodes) + 1) / 2:
                                with self.lock:
                                    # Double-check we're still a candidate for this term
                                    if self.state == NodeState.CANDIDATE and self.current_term == response.get('term', 0):
                                        logger.info(f"Node {self.id} won election with {votes_received} votes")
                                        self._become_leader()
                except Exception as e:
                    logger.warning(f"Failed to request vote from {node}: {e}")
            
            # Start a thread for each node to request votes in parallel
            vote_threads = []
            for node in self.cluster_nodes:
                thread = threading.Thread(target=request_vote_from_node, args=(node,))
                thread.daemon = True
                thread.start()
                vote_threads.append(thread)
            
            # Set a timeout for the election
            election_end_time = time.time() + self.election_timeout
            
            # Wait for all threads to complete or timeout
            for thread in vote_threads:
                remaining_time = max(0, election_end_time - time.time())
                thread.join(timeout=remaining_time)
                if time.time() >= election_end_time:
                    break
            
            # If we didn't get enough votes and we're still a candidate, log the result
            with self.lock:
                if self.state == NodeState.CANDIDATE and votes_received <= (len(self.cluster_nodes) + 1) / 2:
                    logger.info(f"Node {self.id} lost election with {votes_received} votes")
                    # We'll start a new election when the election timeout expires in the main loop

    def _become_leader(self):
        """Transition to leader state."""
        with self.lock:
            if self.state != NodeState.CANDIDATE:
                return
            
            self.state = NodeState.LEADER
            
            # Initialize next_index and match_index
            last_log_index = self.log_storage.get_last_log_index()
            self.next_index = {node: last_log_index + 1 for node in self.cluster_nodes}
            self.match_index = {node: 0 for node in self.cluster_nodes}
            
            logger.info(f"Node {self.id} became leader for term {self.current_term}")
            
            # Reset heartbeat timer to ensure consistent timing
            self.last_heartbeat = time.time()
            
            # Small delay before sending first heartbeat to ensure stability
            time.sleep(0.05)
            
            # Send initial heartbeats
            self._send_heartbeats()

    def _send_heartbeats(self):
        """Send AppendEntries RPCs to all other nodes."""
        for node in self.cluster_nodes:
            threading.Thread(target=self._send_append_entries, args=(node,)).start()

    def _send_append_entries(self, node):
        """Send AppendEntries RPC to a specific node."""
        with self.lock:
            next_idx = self.next_index.get(node, 1)
            prev_log_index = next_idx - 1
            prev_log_term = 0
            
            if prev_log_index > 0:
                prev_log_entry = self.log_storage.get_log_entry(prev_log_index)
                if prev_log_entry:
                    prev_log_term = prev_log_entry.term
            
            # Get entries to send
            entries = self.log_storage.get_entries_from(next_idx)
            entries_dict = [entry.to_dict() for entry in entries]
            
            # Prepare request
            request = {
                'term': self.current_term,
                'leader_id': self.id,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries_dict,
                'leader_commit': self.commit_index
            }
            
            try:
                response = self._send_rpc(node, 'append_entries', request)
                
                if response.get('term', 0) > self.current_term:
                    self._update_term(response['term'])
                    return
                
                if response.get('success', False):
                    # Update indices
                    if entries:
                        self.next_index[node] = entries[-1].index + 1
                        self.match_index[node] = entries[-1].index
                        
                        # Check if we can commit entries
                        self._update_commit_index()
                else:
                    # Decrement next_index and retry
                    if self.next_index[node] > 1:
                        self.next_index[node] -= 1
            except Exception as e:
                logger.warning(f"Failed to send AppendEntries to {node}: {e}")

    def _update_commit_index(self):
        """Update commit_index based on match_index."""
        with self.lock:
            if self.state != NodeState.LEADER:
                return
            
            # Get sorted match indices
            match_indices = sorted([self.match_index[node] for node in self.cluster_nodes])
            
            # Find median match index (majority)
            majority_idx = len(match_indices) // 2
            new_commit_index = match_indices[majority_idx]
            
            # Only commit if entry is from current term
            if new_commit_index > self.commit_index:
                entry = self.log_storage.get_log_entry(new_commit_index)
                if entry and entry.term == self.current_term:
                    self.commit_index = new_commit_index
                    logger.info(f"Leader {self.id} updated commit_index to {self.commit_index}")

    def _apply_committed_entries(self):
        """Apply committed entries to the state machine."""
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log_storage.get_log_entry(self.last_applied)
                if entry:
                    result = self.state_machine.apply(entry.command)
                    logger.debug(f"Applied entry {self.last_applied}: {entry.command}")

    def _handle_rpc(self, client_socket):
        """Handle an incoming RPC."""
        try:
            # Receive data
            data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
            
            if not data:
                return
            
            # Parse request
            request = json.loads(data.decode('utf-8'))
            
            # Handle different RPC types
            response = {}
            if request.get('type') == 'request_vote':
                response = self._handle_request_vote(request)
            elif request.get('type') == 'append_entries':
                response = self._handle_append_entries(request)
            
            # Send response
            client_socket.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error handling RPC: {e}")
        finally:
            client_socket.close()

    def _handle_request_vote(self, request):
        """Handle a RequestVote RPC."""
        with self.lock:
            term = request.get('term', 0)
            candidate_id = request.get('candidate_id', '')
            last_log_index = request.get('last_log_index', 0)
            last_log_term = request.get('last_log_term', 0)
            
            # If term is outdated, reject
            if term < self.current_term:
                return {'term': self.current_term, 'vote_granted': False}
            
            # If term is newer, update our term
            if term > self.current_term:
                self._update_term(term)
            
            # Check if we can vote for this candidate
            vote_granted = False
            if (self.voted_for is None or self.voted_for == candidate_id) and self._is_log_up_to_date(last_log_index, last_log_term):
                self.voted_for = candidate_id
                vote_granted = True
                self.last_heartbeat = time.time()  # Reset election timeout
            
            return {'term': self.current_term, 'vote_granted': vote_granted}

    def _handle_append_entries(self, request):
        """Handle an AppendEntries RPC."""
        with self.lock:
            term = request.get('term', 0)
            leader_id = request.get('leader_id', '')
            prev_log_index = request.get('prev_log_index', 0)
            prev_log_term = request.get('prev_log_term', 0)
            entries = request.get('entries', [])
            leader_commit = request.get('leader_commit', 0)
            
            # If term is outdated, reject
            if term < self.current_term:
                return {'term': self.current_term, 'success': False}
            
            # Reset election timeout
            self.last_heartbeat = time.time()
            
            # Mark that we've checked for a leader (since we've received a heartbeat)
            self.checked_for_leader = True
            
            # If term is newer or we're a candidate, become follower
            if term > self.current_term or self.state == NodeState.CANDIDATE:
                self._update_term(term)
                logger.info(f"Node {self.id} received AppendEntries from leader {leader_id} with term {term}")
            
            # If we're a leader and the term is equal or greater, step down
            if self.state == NodeState.LEADER and term >= self.current_term:
                self.state = NodeState.FOLLOWER
                # Reset election timeout to avoid immediate re-election
                self.election_timeout = self._get_random_election_timeout() * 1.5  # Longer timeout after stepping down
                logger.info(f"Node {self.id} stepping down as leader due to AppendEntries from {leader_id} with term {term}")
            
            # Check if previous log entry matches
            if prev_log_index > 0:
                prev_entry = self.log_storage.get_log_entry(prev_log_index)
                if not prev_entry or prev_entry.term != prev_log_term:
                    return {'term': self.current_term, 'success': False}
            
            # Process entries
            for entry_dict in entries:
                entry = LogEntry.from_dict(entry_dict)
                
                # Check for conflicts
                existing_entry = self.log_storage.get_log_entry(entry.index)
                if existing_entry and existing_entry.term != entry.term:
                    # Delete this and all following entries
                    self.log_storage.delete_entries_from(entry.index)
                
                # Append entry if it doesn't exist
                if not existing_entry:
                    self.log_storage.append_entry(entry)
            
            # Update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.log_storage.get_last_log_index())
            
            return {'term': self.current_term, 'success': True}

    def _update_term(self, term):
        """Update current term and reset state."""
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = NodeState.FOLLOWER
                logger.info(f"Node {self.id} updated term to {term}, becoming follower")

    def _is_log_up_to_date(self, last_log_index, last_log_term):
        """Check if candidate's log is at least as up-to-date as ours."""
        our_last_index = self.log_storage.get_last_log_index()
        our_last_term = 0
        
        if our_last_index > 0:
            our_last_entry = self.log_storage.get_log_entry(our_last_index)
            if our_last_entry:
                our_last_term = our_last_entry.term
        
        if last_log_term > our_last_term:
            return True
        elif last_log_term == our_last_term and last_log_index >= our_last_index:
            return True
        
        return False

    def _send_request_vote_rpc(self, node):
        """Send RequestVote RPC to a specific node."""
        last_log_index = self.log_storage.get_last_log_index()
        last_log_term = 0
        
        if last_log_index > 0:
            last_entry = self.log_storage.get_log_entry(last_log_index)
            if last_entry:
                last_log_term = last_entry.term
        
        request = {
            'type': 'request_vote',
            'term': self.current_term,
            'candidate_id': self.id,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term
        }
        
        return self._send_rpc(node, 'request_vote', request)

    def _send_rpc(self, node, rpc_type, request):
        """Send an RPC to a specific node."""
        request['type'] = rpc_type
        
        try:
            host, port = node.split(':')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, int(port)))
                s.sendall(json.dumps(request).encode('utf-8'))
                s.shutdown(socket.SHUT_WR)
                
                data = b''
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                
                return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.warning(f"Failed to send RPC to {node}: {e}")
            return {}

    def _get_random_election_timeout(self):
        """Get a random election timeout.
        
        Using a wider range to reduce the chance of simultaneous elections.
        The minimum timeout is increased to reduce unnecessary elections.
        """
        return random.uniform(1.5, 3.0)

    def propose_command(self, command):
        """Propose a command to the Raft cluster."""
        with self.lock:
            if self.state != NodeState.LEADER:
                return {'success': False, 'error': 'Not leader', 'leader': self._get_leader_address()}
            
            # Create log entry
            entry = LogEntry(self.current_term, command)
            entry.index = self.log_storage.append_entry(entry)
            
            logger.info(f"Leader {self.id} proposing command: {command}")
            
            # Wait for entry to be committed
            start_time = time.time()
            timeout = 5.0  # 5 seconds timeout
            
            while time.time() - start_time < timeout:
                if self.commit_index >= entry.index:
                    return {'success': True, 'index': entry.index}
                time.sleep(0.1)
            
            return {'success': False, 'error': 'Timeout waiting for commit'}

    def _get_leader_address(self):
        """Get the address of the current leader."""
        if self.state == NodeState.LEADER:
            return self.address
        
        # For simplicity, we don't track the leader
        # In a real implementation, we would store the leader ID from AppendEntries
        return None

    def get_state_machine_state(self):
        """Get the current state of the state machine."""
        with self.lock:
            return self.state_machine.get_state()