# import json
# import logging
# import os
# import random
# import threading
# import time
# from enum import Enum
# import socket

# from .state_machine import RaftStateMachine
# from .storage import LogStorage

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger("RaftNode")

# class NodeState(Enum):
#     FOLLOWER = 0
#     CANDIDATE = 1
#     LEADER = 2

# class LogEntry:
#     def __init__(self, term, command, index=None):
#         self.term = term
#         self.command = command
#         self.index = index  # Set when appended to log

#     def to_dict(self):
#         return {
#             'term': self.term,
#             'command': self.command,
#             'index': self.index
#         }

#     @classmethod
#     def from_dict(cls, data):
#         entry = cls(data['term'], data['command'])
#         entry.index = data['index']
#         return entry

# class RaftNode:
#     def __init__(self, node_id, port, cluster_nodes):
#         self.id = node_id
#         self.address = f"localhost:{port}"  # Simplified address format
#         self.port = port
#         # Cluster nodes excluding self
#         self.cluster_nodes = [node for node in cluster_nodes if node != self.address]
        
#         # Raft state variables
#         self.state = NodeState.FOLLOWER
#         self.current_term = 0
#         self.voted_for = None
#         self.election_timeout = self._get_random_election_timeout()
#         self.last_heartbeat = time.time()
#         self.checked_for_leader = False
        
#         # Log and commit state
#         self.log_storage = LogStorage(f"data/log/{self.id}")
#         self.commit_index = 0
#         self.last_applied = 0
        
#         # Leader volatile state (only valid when leader)
#         self.next_index = {node: 1 for node in self.cluster_nodes}
#         self.match_index = {node: 0 for node in self.cluster_nodes}
        
#         # FSM – you must implement the FSM in RaftStateMachine manually
#         self.state_machine = RaftStateMachine()
        
#         # Concurrency lock
#         self.lock = threading.RLock()
        
#         # Set up server socket for RPC
#         self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#         self.server_socket.bind(('0.0.0.0', port))
#         self.running = False
        
#         logger.info(f"Initialized node {self.id} on {self.address}")

#     def start(self):
#         """Start the Raft node: begin RPC server and main loop."""
#         self.running = True
        
#         # Check for an existing leader before election starts
#         self._check_for_existing_leader()
        
#         # Start the main loop in a separate thread
#         main_thread = threading.Thread(target=self._main_loop)
#         main_thread.daemon = True
#         main_thread.start()
        
#         # Start server to handle incoming RPCs
#         self.server_socket.listen(5)
#         while self.running:
#             try:
#                 client_socket, _ = self.server_socket.accept()
#                 client_handler = threading.Thread(target=self._handle_rpc, args=(client_socket,))
#                 client_handler.daemon = True
#                 client_handler.start()
#             except Exception as e:
#                 if self.running:
#                     logger.error(f"Error accepting connection: {e}")
#         logger.info(f"Node {self.id} stopped")

#     def stop(self):
#         """Stop the Raft node."""
#         self.running = False
#         self.server_socket.close()

#     def _check_for_existing_leader(self):
#         """Check for an existing leader in the cluster before starting election timeout."""
#         logger.info(f"Node {self.id} checking for existing leader in the cluster")
#         for node in self.cluster_nodes:
#             try:
#                 request = {
#                     'type': 'append_entries',
#                     'term': self.current_term,
#                     'leader_id': '',  # Unknown at this point
#                     'prev_log_index': 0,
#                     'prev_log_term': 0,
#                     'entries': [],
#                     'leader_commit': self.commit_index
#                 }
#                 response = self._send_rpc(node, 'append_entries', request)
#                 if response and 'term' in response and response['term'] > self.current_term:
#                     with self.lock:
#                         self._update_term(response['term'])
#                         self.last_heartbeat = time.time()  # Reset election timer
#                         self.checked_for_leader = True
#                         logger.info(f"Node {self.id} found an existing leader (or higher term), resetting timeout")
#                         return True
#             except Exception as e:
#                 logger.warning(f"Failed to check for leader with node {node}: {e}")
#         logger.info(f"Node {self.id} did not find an existing leader")
#         self.checked_for_leader = True
#         return False

#     def _main_loop(self):
#         """Main loop: handle election timeouts, heartbeat sending, and log application."""
#         while self.running:
#             with self.lock:
#                 current_time = time.time()
#                 if self.state == NodeState.FOLLOWER:
#                     if current_time - self.last_heartbeat > self.election_timeout:
#                         if self.checked_for_leader:
#                             logger.info(f"Node {self.id} election timeout, becoming candidate")
#                             self._become_candidate()
#                         else:
#                             self._check_for_existing_leader()
#                 elif self.state == NodeState.CANDIDATE:
#                     if current_time - self.last_heartbeat > self.election_timeout:
#                         logger.info(f"Node {self.id} election timeout as candidate, starting new election")
#                         self._start_election()
#                 elif self.state == NodeState.LEADER:
#                     # Heartbeat interval set to 50ms
#                     if current_time - self.last_heartbeat > 0.05:
#                         self._send_heartbeats()
#                         self.last_heartbeat = current_time
#                 self._apply_committed_entries()
#             time.sleep(0.01)

#     def _become_candidate(self):
#         """Transition to candidate state and start election."""
#         self.state = NodeState.CANDIDATE
#         self._start_election()

#     def _start_election(self):
#         """Conduct a leader election."""
#         with self.lock:
#             if self.state != NodeState.CANDIDATE:
#                 return  # Election already aborted
#             self.current_term += 1
#             self.voted_for = self.id
#             self.election_timeout = self._get_random_election_timeout()
#             self.last_heartbeat = time.time()
#             logger.info(f"Node {self.id} starting election for term {self.current_term}")
#             votes_lock = threading.Lock()
#             votes_received = 1  # Vote for self

#             def request_vote_from_node(node):
#                 nonlocal votes_received
#                 try:
#                     response = self._send_request_vote_rpc(node)
#                     if response.get('term', 0) > self.current_term:
#                         with self.lock:
#                             self._update_term(response['term'])
#                         return
#                     if response.get('vote_granted', False):
#                         with votes_lock:
#                             votes_received += 1
#                             if votes_received > (len(self.cluster_nodes) + 1) / 2:
#                                 with self.lock:
#                                     if self.state == NodeState.CANDIDATE:
#                                         logger.info(f"Node {self.id} won election with {votes_received} votes")
#                                         self._become_leader()
#                 except Exception as e:
#                     logger.warning(f"Failed to request vote from {node}: {e}")

#             vote_threads = []
#             for node in self.cluster_nodes:
#                 t = threading.Thread(target=request_vote_from_node, args=(node,))
#                 t.daemon = True
#                 t.start()
#                 vote_threads.append(t)
#             election_deadline = time.time() + self.election_timeout
#             for t in vote_threads:
#                 remaining = max(0, election_deadline - time.time())
#                 t.join(timeout=remaining)
#             with self.lock:
#                 if self.state == NodeState.CANDIDATE and votes_received <= (len(self.cluster_nodes) + 1) / 2:
#                     logger.info(f"Node {self.id} lost election with {votes_received} votes")
#                     # Wait for the next election timeout to trigger another election

#     def _become_leader(self):
#         """Transition to leader state."""
#         with self.lock:
#             if self.state != NodeState.CANDIDATE:
#                 return
#             self.state = NodeState.LEADER
#             last_log_index = self.log_storage.get_last_log_index()
#             self.next_index = {node: last_log_index + 1 for node in self.cluster_nodes}
#             self.match_index = {node: 0 for node in self.cluster_nodes}
#             logger.info(f"Node {self.id} became leader for term {self.current_term}")
#             self.last_heartbeat = time.time()
#             time.sleep(0.05)
#             self._send_heartbeats()

#     def _send_heartbeats(self):
#         """Send empty AppendEntries (heartbeats) to all followers."""
#         for node in self.cluster_nodes:
#             threading.Thread(target=self._send_append_entries, args=(node,)).start()

#     def _send_append_entries(self, node):
#         """Send AppendEntries RPC to a specific follower."""
#         with self.lock:
#             next_idx = self.next_index.get(node, 1)
#             prev_log_index = next_idx - 1
#             prev_log_term = 0
#             if prev_log_index > 0:
#                 prev_log_entry = self.log_storage.get_log_entry(prev_log_index)
#                 if prev_log_entry:
#                     prev_log_term = prev_log_entry.term
#             entries = self.log_storage.get_entries_from(next_idx)
#             entries_dict = [entry.to_dict() for entry in entries]
#             request = {
#                 'term': self.current_term,
#                 'leader_id': self.id,
#                 'prev_log_index': prev_log_index,
#                 'prev_log_term': prev_log_term,
#                 'entries': entries_dict,
#                 'leader_commit': self.commit_index
#             }
#         try:
#             response = self._send_rpc(node, 'append_entries', request)
#             with self.lock:
#                 if response.get('term', 0) > self.current_term:
#                     self._update_term(response['term'],reason="higher term in RPC response")
#                     return
#                 if response.get('success', False):
#                     if entries:
#                         self.next_index[node] = entries[-1].index + 1
#                         self.match_index[node] = entries[-1].index
#                         self._update_commit_index()
#                 else:
#                     if self.next_index[node] > 1:
#                         self.next_index[node] -= 1
#         except Exception as e:
#             logger.warning(f"Failed to send AppendEntries to {node}: {e}")

#     def _update_commit_index(self):
#         """Update commit_index based on majority match_index."""
#         with self.lock:
#             if self.state != NodeState.LEADER:
#                 return
#             match_values = list(self.match_index.values())
#             match_values.append(self.log_storage.get_last_log_index())  # Leader’s own log
#             match_values.sort()
#             majority_commit = match_values[len(match_values) // 2]
#             if majority_commit > self.commit_index:
#                 entry = self.log_storage.get_log_entry(majority_commit)
#                 if entry and entry.term == self.current_term:
#                     self.commit_index = majority_commit
#                     logger.info(f"Leader {self.id} updated commit_index to {self.commit_index}")

#     def _apply_committed_entries(self):
#         """Apply committed log entries to the state machine."""
#         with self.lock:
#             while self.last_applied < self.commit_index:
#                 self.last_applied += 1
#                 entry = self.log_storage.get_log_entry(self.last_applied)
#                 if entry:
#                     result = self.state_machine.apply(entry.command)
#                     logger.debug(f"Applied entry {self.last_applied}: {entry.command}")

#     def _handle_rpc(self, client_socket):
#         """Handle an incoming RPC request."""
#         logger.debug(f"Received RPC: {client_socket}")    #this is for debug
#         try:
#             data = b""
#             while True:
#                 chunk = client_socket.recv(4096)
#                 if not chunk:
#                     break
#                 data += chunk
#             if not data:
#                 return
#             request = json.loads(data.decode('utf-8'))
#             response = {}
#             if request.get('type') == 'request_vote':
#                 response = self._handle_request_vote(request)
#             elif request.get('type') == 'append_entries':
#                 response = self._handle_append_entries(request)
#             client_socket.sendall(json.dumps(response).encode('utf-8'))
#         except Exception as e:
#             logger.error(f"Error handling RPC: {e}")
#         finally:
#             client_socket.close()

#     def _handle_request_vote(self, request):
#         if request['term'] > self.current_term + 10:  # Reject unreasonable term jumps
#             logger.warning(f"Rejected invalid term jump from {request['candidate_id']}")
#             return {'term': self.current_term, 'vote_granted': False}
#         """Process a RequestVote RPC."""
#         with self.lock:
#             term = request.get('term', 0)
#             candidate_id = request.get('candidate_id', '')
#             last_log_index = request.get('last_log_index', 0)
#             last_log_term = request.get('last_log_term', 0)
#             if term < self.current_term:
#                 return {'term': self.current_term, 'vote_granted': False}
#             if term > self.current_term:
#                 self._update_term(term)
#             vote_granted = False
#             if (self.voted_for is None or self.voted_for == candidate_id) and self._is_log_up_to_date(last_log_index, last_log_term):
#                 self.voted_for = candidate_id
#                 vote_granted = True
#                 self.last_heartbeat = time.time()  # Reset timeout
#             return {'term': self.current_term, 'vote_granted': vote_granted}

#     def _handle_append_entries(self, request):
#         """Process an AppendEntries RPC (heartbeat and log replication)."""
#         with self.lock:
#             term = request.get('term', 0)
#             leader_id = request.get('leader_id', '')
#             prev_log_index = request.get('prev_log_index', 0)
#             prev_log_term = request.get('prev_log_term', 0)
#             entries = request.get('entries', [])
#             leader_commit = request.get('leader_commit', 0)
#             if term < self.current_term:
#                 return {'term': self.current_term, 'success': False}
#             self.last_heartbeat = time.time()
#             self.checked_for_leader = True
#             if term > self.current_term or self.state == NodeState.CANDIDATE:
#                 self._update_term(term)
#                 logger.info(f"Node {self.id} received AppendEntries from leader {leader_id} with term {term}")
#             if self.state == NodeState.LEADER and term >= self.current_term:
#                 self.state = NodeState.FOLLOWER
#                 self.election_timeout = self._get_random_election_timeout() * 1.5
#                 logger.info(f"Node {self.id} stepping down as leader due to AppendEntries from {leader_id} with term {term}")
#             if prev_log_index > 0:
#                 prev_entry = self.log_storage.get_log_entry(prev_log_index)
#                 if not prev_entry or prev_entry.term != prev_log_term:
#                     return {'term': self.current_term, 'success': False}
#             for entry_dict in entries:
#                 entry = LogEntry.from_dict(entry_dict)
#                 existing_entry = self.log_storage.get_log_entry(entry.index)
#                 if existing_entry and existing_entry.term != entry.term:
#                     self.log_storage.delete_entries_from(entry.index)
#                 if not existing_entry:
#                     self.log_storage.append_entry(entry)
#             if leader_commit > self.commit_index:
#                 self.commit_index = min(leader_commit, self.log_storage.get_last_log_index())
#             return {'term': self.current_term, 'success': True}

#     # def _update_term(self, term):
#     #     """Update current term and revert to follower."""
#     #     with self.lock:
#     #         if term > self.current_term:
#     #             self.current_term = term
#     #             self.voted_for = None
#     #             self.state = NodeState.FOLLOWER
#     #             logger.info(f"Node {self.id} updated term to {term}, becoming follower")

#     def _update_term(self, term, reason="",source_node=None):
#         logger.info(f"Leader {self.id} stepping down due to term update from {source_node} (new term: {term})")
#         with self.lock:
#             if term > self.current_term:
#                 logger.info(f"Leader {self.id} stepping down due to {reason} (new term: {term})")
#                 self.current_term = term
#                 self.voted_for = None
#                 self.state = NodeState.FOLLOWER

#     def _is_log_up_to_date(self, last_log_index, last_log_term):
#         """Determine if candidate's log is at least as up-to-date as this node's log."""
#         our_last_index = self.log_storage.get_last_log_index()
#         our_last_term = 0
#         if our_last_index > 0:
#             our_last_entry = self.log_storage.get_log_entry(our_last_index)
#             if our_last_entry:
#                 our_last_term = our_last_entry.term
#         if last_log_term > our_last_term:
#             return True
#         elif last_log_term == our_last_term and last_log_index >= our_last_index:
#             return True
#         return False

#     def _send_request_vote_rpc(self, node):
#         """Send a RequestVote RPC to a specified node."""
#         last_log_index = self.log_storage.get_last_log_index()
#         last_log_term = 0
#         if last_log_index > 0:
#             last_entry = self.log_storage.get_log_entry(last_log_index)
#             if last_entry:
#                 last_log_term = last_entry.term
#         request = {
#             'type': 'request_vote',
#             'term': self.current_term,
#             'candidate_id': self.id,
#             'last_log_index': last_log_index,
#             'last_log_term': last_log_term
#         }
#         return self._send_rpc(node, 'request_vote', request)

#     def _send_rpc(self, node, rpc_type, request):
#         """Send an RPC to a given node and return the response."""
#         logger.debug(f"Sending {rpc_type} to {node}: {request}")            #this is by me for debug
#         request['type'] = rpc_type
#         try:
#             host, port = node.split(':')
#             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                 s.connect((host, int(port)))
#                 s.sendall(json.dumps(request).encode('utf-8'))
#                 s.shutdown(socket.SHUT_WR)
#                 data = b""
#                 while True:
#                     chunk = s.recv(4096)
#                     if not chunk:
#                         break
#                     data += chunk
#                 return json.loads(data.decode('utf-8'))
#         except Exception as e:
#             logger.warning(f"Failed to send RPC to {node}: {e}")
#             return {}

#     def _get_random_election_timeout(self):
#         """Return a random election timeout between 1.5 and 3.0 seconds."""
#         return random.uniform(5.0, 6.0)

#     def propose_command(self, command):
#         """Propose a command to the cluster (only valid if node is leader)."""
#         with self.lock:
#             if self.state != NodeState.LEADER:
#                 return {'success': False, 'error': 'Not leader', 'leader': self._get_leader_address()}
#             entry = LogEntry(self.current_term, command)
#             entry.index = self.log_storage.append_entry(entry)
#             logger.info(f"Leader {self.id} proposing command: {command}")
#             start_time = time.time()
#             timeout = 5.0  # seconds
#             while time.time() - start_time < timeout:   
#                 if self.commit_index >= entry.index:
#                     return {'success': True, 'index': entry.index}
#                 time.sleep(0.1)
#             return {'success': False, 'error': 'Timeout waiting for commit'}

#     def _get_leader_address(self):
#         """Return the current leader's address if known."""
#         if self.state == NodeState.LEADER:
#             return self.address
#         return None

#     def get_state_machine_state(self):
#         """Return the current state of the state machine."""
#         with self.lock:
#             return self.state_machine.get_state()
import json
import logging
import os
import random
import threading
import time
from enum import Enum
import socket

from .state_machine import RaftStateMachine
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
        self.index = index  # Set when appended to log

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
        self.address = f"localhost:{port}"  # Simplified address format
        self.port = port
        # Cluster nodes excluding self
        self.cluster_nodes = [node for node in cluster_nodes if node != self.address]
        
        # Raft state variables
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.election_timeout = self._get_random_election_timeout()
        self.last_heartbeat = time.time()
        self.checked_for_leader = False
        
        # Log and commit state
        self.log_storage = LogStorage(f"data/log/{self.id}")
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader volatile state (only valid when leader)
        self.next_index = {node: 1 for node in self.cluster_nodes}
        self.match_index = {node: 0 for node in self.cluster_nodes}
        
        # FSM – you must implement the FSM in RaftStateMachine manually
        self.state_machine = RaftStateMachine()
        
        # Concurrency lock
        self.lock = threading.RLock()
        
        # Set up server socket for RPC
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', port))
        self.running = False
        
        logger.info(f"Initialized node {self.id} on {self.address}")

    def start(self):
        """Start the Raft node: begin RPC server and main loop."""
        self.running = True
        
        # Check for an existing leader before election starts
        self._check_for_existing_leader()
        
        # Start the main loop in a separate thread
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
        """Check for an existing leader in the cluster before starting election timeout."""
        logger.info(f"Node {self.id} checking for existing leader in the cluster")
        for node in self.cluster_nodes:
            try:
                request = {
                    'type': 'append_entries',
                    'term': self.current_term,
                    'leader_id': '',  # Unknown at this point
                    'prev_log_index': 0,
                    'prev_log_term': 0,
                    'entries': [],
                    'leader_commit': self.commit_index
                }
                response = self._send_rpc(node, 'append_entries', request)
                if response and 'term' in response and response['term'] > self.current_term:
                    with self.lock:
                        self._update_term(response['term'])
                        self.last_heartbeat = time.time()  # Reset election timer
                        self.checked_for_leader = True
                        logger.info(f"Node {self.id} found an existing leader (or higher term), resetting timeout")
                        return True
            except Exception as e:
                logger.warning(f"Failed to check for leader with node {node}: {e}")
        logger.info(f"Node {self.id} did not find an existing leader")
        self.checked_for_leader = True
        return False

    def _main_loop(self):
        """Main loop: handle election timeouts, heartbeat sending, and log application."""
        while self.running:
            with self.lock:
                current_time = time.time()
                if self.state == NodeState.FOLLOWER:
                    if current_time - self.last_heartbeat > self.election_timeout:
                        if self.checked_for_leader:
                            logger.info(f"Node {self.id} election timeout, becoming candidate")
                            self._become_candidate()
                        else:
                            self._check_for_existing_leader()
                elif self.state == NodeState.CANDIDATE:
                    if current_time - self.last_heartbeat > self.election_timeout:
                        logger.info(f"Node {self.id} election timeout as candidate, starting new election")
                        self._start_election()
                elif self.state == NodeState.LEADER:
                    # Heartbeat interval set to 50ms
                    if current_time - self.last_heartbeat > 0.05:
                        self._send_heartbeats()
                        self.last_heartbeat = current_time
                self._apply_committed_entries()
            time.sleep(0.01)

    def _become_candidate(self):
        """Transition to candidate state and start election."""
        self.state = NodeState.CANDIDATE
        self._start_election()

    def _start_election(self):
        """Conduct a leader election."""
        with self.lock:
            if self.state != NodeState.CANDIDATE:
                return  # Election already aborted
            self.current_term += 1
            self.voted_for = self.id
            self.election_timeout = self._get_random_election_timeout()
            self.last_heartbeat = time.time()
            logger.info(f"Node {self.id} starting election for term {self.current_term}")
            votes_lock = threading.Lock()
            votes_received = 1  # Vote for self

            def request_vote_from_node(node):
                nonlocal votes_received
                try:
                    response = self._send_request_vote_rpc(node)
                    if response.get('term', 0) > self.current_term:
                        with self.lock:
                            self._update_term(response['term'])
                        return
                    if response.get('vote_granted', False):
                        with votes_lock:
                            votes_received += 1
                            if votes_received > (len(self.cluster_nodes) + 1) / 2:
                                with self.lock:
                                    if self.state == NodeState.CANDIDATE:
                                        logger.info(f"Node {self.id} won election with {votes_received} votes")
                                        self._become_leader()
                except Exception as e:
                    logger.warning(f"Failed to request vote from {node}: {e}")

            vote_threads = []
            for node in self.cluster_nodes:
                t = threading.Thread(target=request_vote_from_node, args=(node,))
                t.daemon = True
                t.start()
                vote_threads.append(t)
            election_deadline = time.time() + self.election_timeout
            for t in vote_threads:
                remaining = max(0, election_deadline - time.time())
                t.join(timeout=remaining)
            with self.lock:
                if self.state == NodeState.CANDIDATE and votes_received <= (len(self.cluster_nodes) + 1) / 2:
                    logger.info(f"Node {self.id} lost election with {votes_received} votes")
                    # Wait for the next election timeout to trigger another election

    def _become_leader(self):
        """Transition to leader state."""
        with self.lock:
            if self.state != NodeState.CANDIDATE:
                return
            self.state = NodeState.LEADER
            last_log_index = self.log_storage.get_last_log_index()
            self.next_index = {node: last_log_index + 1 for node in self.cluster_nodes}
            self.match_index = {node: 0 for node in self.cluster_nodes}
            logger.info(f"Node {self.id} became leader for term {self.current_term}")
            self.last_heartbeat = time.time()
            time.sleep(0.05)
            self._send_heartbeats()

    def _send_heartbeats(self):
        """Send empty AppendEntries (heartbeats) to all followers."""
        for node in self.cluster_nodes:
            threading.Thread(target=self._send_append_entries, args=(node,)).start()

    def _send_append_entries(self, node):
        """Send AppendEntries RPC to a specific follower and log its actions."""
        with self.lock:
            next_idx = self.next_index.get(node, 1)
            prev_log_index = next_idx - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_log_entry = self.log_storage.get_log_entry(prev_log_index)
                if prev_log_entry:
                    prev_log_term = prev_log_entry.term
            entries = self.log_storage.get_entries_from(next_idx)
            entries_dict = [entry.to_dict() for entry in entries]
            request = {
                'term': self.current_term,
                'leader_id': self.id,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries_dict,
                'leader_commit': self.commit_index
            }
        # Log what the follower is receiving (if any)
        # logger.info(f"Node {node} (follower) should receive AppendEntries with {len(entries_dict)} entries")
        try:
            response = self._send_rpc(node, 'append_entries', request)
            with self.lock:
                if response.get('term', 0) > self.current_term:
                    self._update_term(response['term'], reason="higher term in RPC response", source_node=node)
                    return
                if response.get('success', False):
                    if entries:
                        self.next_index[node] = entries[-1].index + 1
                        self.match_index[node] = entries[-1].index
                        self._update_commit_index()
                else:
                    if self.next_index[node] > 1:
                        self.next_index[node] -= 1
        except Exception as e:
            logger.warning(f"Failed to send AppendEntries to {node}: {e}")

    def _update_commit_index(self):
        """Update commit_index based on majority match_index and log the committed entry."""
        with self.lock:
            if self.state != NodeState.LEADER:
                return
            match_values = list(self.match_index.values())
            match_values.append(self.log_storage.get_last_log_index())  # Leader’s own log
            match_values.sort()
            majority_commit = match_values[len(match_values) // 2]
            if majority_commit > self.commit_index:
                entry = self.log_storage.get_log_entry(majority_commit)
                if entry and entry.term == self.current_term:
                    self.commit_index = majority_commit
                    logger.info(f"Leader {self.id} updated commit_index to {self.commit_index}")
                    # Call commit_log_entries to log the commit and apply the command
                    self.commit_log_entries()

    def commit_log_entries(self):
        """Commit new log entries and apply them to the state machine."""
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log_storage.get_log_entry(self.last_applied)
                if entry:
                    logger.info(f"Committing entry at index {self.last_applied}: {entry.to_dict()}")
                    result = self.state_machine.apply(entry.command)
                    logger.debug(f"Applied entry {self.last_applied}: {result}")

    def _apply_committed_entries(self):
        """Apply committed log entries to the state machine (if not already applied)."""
        # This function can be used if you want continuous background application.
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log_storage.get_log_entry(self.last_applied)
                if entry:
                    result = self.state_machine.apply(entry.command)
                    logger.debug(f"Applied entry {self.last_applied}: {result}")

    def _handle_rpc(self, client_socket):
        """Handle an incoming RPC request."""
        logger.debug(f"Received RPC connection from {client_socket.getpeername()}")
        try:
            data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
            if not data:
                return
            request = json.loads(data.decode('utf-8'))
            response = {}
            if request.get('type') == 'request_vote':
                response = self._handle_request_vote(request)
            elif request.get('type') == 'append_entries':
                response = self._handle_append_entries(request)
            client_socket.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error handling RPC: {e}")
        finally:
            client_socket.close()

    def _handle_request_vote(self, request):
        """Process a RequestVote RPC."""
        with self.lock:
            term = request.get('term', 0)
            candidate_id = request.get('candidate_id', '')
            last_log_index = request.get('last_log_index', 0)
            last_log_term = request.get('last_log_term', 0)
            if term < self.current_term:
                return {'term': self.current_term, 'vote_granted': False}
            if term > self.current_term:
                self._update_term(term)
            vote_granted = False
            if (self.voted_for is None or self.voted_for == candidate_id) and self._is_log_up_to_date(last_log_index, last_log_term):
                self.voted_for = candidate_id
                vote_granted = True
                self.last_heartbeat = time.time()  # Reset timeout
            return {'term': self.current_term, 'vote_granted': vote_granted}

    def _handle_append_entries(self, request):
        """Process an AppendEntries RPC (heartbeat and log replication)."""
        with self.lock:
            term = request.get('term', 0)
            leader_id = request.get('leader_id', '')
            prev_log_index = request.get('prev_log_index', 0)
            prev_log_term = request.get('prev_log_term', 0)
            entries = request.get('entries', [])
            leader_commit = request.get('leader_commit', 0)
            if term < self.current_term:
                return {'term': self.current_term, 'success': False}
            self.last_heartbeat = time.time()
            self.checked_for_leader = True
            if term > self.current_term or self.state == NodeState.CANDIDATE:
                self._update_term(term)
                logger.info(f"Node {self.id} received AppendEntries from leader {leader_id} with term {term}")
            if self.state == NodeState.LEADER and term >= self.current_term:
                self.state = NodeState.FOLLOWER
                self.election_timeout = self._get_random_election_timeout() * 1.5
                logger.info(f"Node {self.id} stepping down as leader due to AppendEntries from {leader_id} with term {term}")
            if prev_log_index > 0:
                prev_entry = self.log_storage.get_log_entry(prev_log_index)
                if not prev_entry or prev_entry.term != prev_log_term:
                    return {'term': self.current_term, 'success': False}
            for entry_dict in entries:
                entry = LogEntry.from_dict(entry_dict)
                existing_entry = self.log_storage.get_log_entry(entry.index)
                if existing_entry and existing_entry.term != entry.term:
                    self.log_storage.delete_entries_from(entry.index)
                if not existing_entry:
                    self.log_storage.append_entry(entry)
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.log_storage.get_last_log_index())
            return {'term': self.current_term, 'success': True}

    def _update_term(self, term, reason="", source_node=None):
        """Update current term and revert to follower."""
        with self.lock:
            if term > self.current_term:
                logger.info(f"Node {self.id} updating term from {self.current_term} to {term} due to {reason}. Source: {source_node}")
                self.current_term = term
                self.voted_for = None
                self.state = NodeState.FOLLOWER

    def _is_log_up_to_date(self, last_log_index, last_log_term):
        """Determine if candidate's log is at least as up-to-date as this node's log."""
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
        """Send a RequestVote RPC to a specified node."""
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
        """Send an RPC to a given node and return the response."""
        logger.debug(f"Sending {rpc_type} to {node}: {request}")
        request['type'] = rpc_type
        try:
            host, port = node.split(':')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, int(port)))
                s.sendall(json.dumps(request).encode('utf-8'))
                s.shutdown(socket.SHUT_WR)
                data = b""
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
        """Return a random election timeout between 5.0 and 6.0 seconds."""
        return random.uniform(5.0, 6.0)

    def propose_command(self, command):
        """Propose a command to the cluster (only valid if node is leader)."""
        with self.lock:
            if self.state != NodeState.LEADER:
                return {'success': False, 'error': 'Not leader', 'leader': self._get_leader_address()}
            # Append command to leader's log
            entry = LogEntry(self.current_term, command)
            entry.index = self.log_storage.append_entry(entry)
            logger.info(f"Leader {self.id} proposing command: {command}")
            logger.info(f"Command appended successfully to leader {self.id}. Current log length: {self.log_storage.get_last_log_index()}")
            # Optionally, send AppendEntries RPCs to all followers immediately
            for node in self.cluster_nodes:
                logger.info(f"Sending AppendEntries RPC to {node}")
                response = self._send_append_entries(node)
                logger.info(f"AppendEntries response from {node}: {response}")
            # Wait for command to be committed
            start_time = time.time()
            timeout = 5.0  # seconds
            while time.time() - start_time < timeout:
                if self.commit_index >= entry.index:
                    return {'success': True, 'index': entry.index}
                time.sleep(0.1)
            return {'success': False, 'error': 'Timeout waiting for commit'}

    def _get_leader_address(self):
        """Return the current leader's address if known."""
        if self.state == NodeState.LEADER:
            return self.address
        return None

    def get_state_machine_state(self):
        """Return the current state of the state machine."""
        with self.lock:
            return self.state_machine.get_state()
