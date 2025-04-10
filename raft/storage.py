import json
import logging
import os
from collections import OrderedDict

# If needed, import LogEntry from node (ensure no circular dependency)
# from .node import LogEntry

logger = logging.getLogger("LogStorage")

# If LogEntry isn't imported from node, you can define a minimal helper here
# But it is preferable to import it if already defined elsewhere.

class LogStorage:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.entries = OrderedDict()  # In-memory cache of log entries
        self.last_index = 0
        os.makedirs(log_dir, exist_ok=True)
        self._load_entries()
        logger.info(f"Initialized log storage at {log_dir} with {len(self.entries)} entries")

    def _load_entries(self):
        """Load log entries from disk into the in-memory cache."""
        try:
            files = [f for f in os.listdir(self.log_dir) if f.startswith('log_') and f.endswith('.json')]
            # Sort files based on numeric index
            files.sort(key=lambda f: int(f.split('_')[1].split('.')[0]))
            for file in files:
                index = int(file.split('_')[1].split('.')[0])
                path = os.path.join(self.log_dir, file)
                with open(path, 'r') as f:
                    entry_data = json.load(f)
                    # Assuming LogEntry is imported or defined; adjust accordingly.
                    from .node import LogEntry  # Use local import to avoid circular issues
                    entry = LogEntry.from_dict(entry_data)
                    self.entries[index] = entry
                    self.last_index = max(self.last_index, index)
        except Exception as e:
            logger.error(f"Error loading log entries: {e}")

    def append_entry(self, entry):
        """Append a new log entry and persist it to disk."""
        index = self.last_index + 1
        entry.index = index
        path = os.path.join(self.log_dir, f'log_{index}.json')
        try:
            with open(path, 'w') as f:
                json.dump(entry.to_dict(), f)
            self.entries[index] = entry
            self.last_index = index
            return index
        except Exception as e:
            logger.error(f"Error appending log entry: {e}")
            return None

    def get_log_entry(self, index):
        """Return a log entry by its index."""
        return self.entries.get(index)

    def get_entries_from(self, start_index):
        """Return a list of log entries starting from the specified index."""
        return [entry for idx, entry in self.entries.items() if idx >= start_index]

    def get_last_log_index(self):
        """Return the index of the last log entry."""
        return self.last_index

    def delete_entries_from(self, start_index):
        """Delete log entries from start_index onwards, both in memory and on disk."""
        indices_to_delete = [idx for idx in list(self.entries.keys()) if idx >= start_index]
        for idx in indices_to_delete:
            path = os.path.join(self.log_dir, f'log_{idx}.json')
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                logger.error(f"Error deleting log file {path}: {e}")
            self.entries.pop(idx, None)
        self.last_index = max(self.entries.keys()) if self.entries else 0
