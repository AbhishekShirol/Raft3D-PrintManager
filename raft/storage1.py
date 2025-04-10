import json
import os
import logging
import shutil
from collections import OrderedDict

logger = logging.getLogger("LogStorage")

class LogStorage:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.entries = OrderedDict()  # In-memory cache of log entries
        self.last_index = 0
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Load existing log entries
        self._load_entries()
        
        logger.info(f"Initialized log storage at {log_dir} with {len(self.entries)} entries")

    def _load_entries(self):
        """Load log entries from disk."""
        try:
            # Find all log files
            files = [f for f in os.listdir(self.log_dir) if f.startswith('log_')]
            
            # Sort by index
            files.sort(key=lambda f: int(f.split('_')[1].split('.')[0]))
            
            # Load each file
            for file in files:
                index = int(file.split('_')[1].split('.')[0])
                path = os.path.join(self.log_dir, file)
                
                with open(path, 'r') as f:
                    entry_data = json.load(f)
                    self.entries[index] = entry_data
                    self.last_index = max(self.last_index, index)
        except Exception as e:
            logger.error(f"Error loading log entries: {e}")

    def append_entry(self, entry):
        """Append a new entry to the log."""
        index = self.last_index + 1
        entry.index = index
        
        # Save entry to disk
        path = os.path.join(self.log_dir, f'log_{index}.json')
        with open(path, 'w') as f:
            json.dump(entry.to_dict(), f)
        
        # Update in-memory cache
        self.entries[index] = entry
        self.last_index = index
        
        return index

    def get_log_entry(self, index):
        """Get a log entry by index."""
        return self.entries.get(index)

    def get_entries_from(self, start_index):
        """Get log entries starting from start_index."""
        return [entry for idx, entry in self.entries.items() if idx >= start_index]

    def get_last_log_index(self):
        """Get the index of the last log entry."""
        return self.last_index

    def delete_entries_from(self, start_index):
        """Delete log entries from start_index onwards."""
        # Remove from disk
        for index in list(self.entries.keys()):
            if index >= start_index:
                path = os.path.join(self.log_dir, f'log_{index}.json')
                if os.path.exists(path):
                    os.remove(path)
                
                # Remove from in-memory cache
                if index in self.entries:
                    del self.entries[index]
        
        # Update last_index
        if self.entries:
            self.last_index = max(self.entries.keys())
        else:
            self.last_index = 0