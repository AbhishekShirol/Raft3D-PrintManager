import os

# Server configurations
DEFAULT_PORT = 8000
DEFAULT_RAFT_PORT = 5000

# Raft configurations
RAFT_HEARTBEAT_TIMEOUT = 0.5  # seconds
RAFT_ELECTION_TIMEOUT_MIN = 1.0  # seconds
RAFT_ELECTION_TIMEOUT_MAX = 2.0  # seconds
RAFT_SNAPSHOT_INTERVAL = 30  # seconds

# Data storage
DATA_DIR = "data"
LOG_DIR = os.path.join(DATA_DIR, "log")
SNAPSHOT_DIR = os.path.join(DATA_DIR, "snapshot")

# Ensure directories exist
for directory in [DATA_DIR, LOG_DIR, SNAPSHOT_DIR]:
    os.makedirs(directory, exist_ok=True)