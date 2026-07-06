"""
Persistent queue backed by a single JSON file.

Each process (server or client) owns exactly one file on disk. A message
is appended as soon as it's created, and removed only after a delivery
confirmation (ACK) arrives. This is what gives the system its
"store-and-forward" durability: even if a process sleeps or a message
sits in transit for a while, the data survives on disk, not just in RAM.
"""
import json
import os


class PersistentQueue:
    def __init__(self, filepath):
        self.filepath = filepath
        if not os.path.exists(self.filepath):
            self._write([])

    def _read(self):
        with open(self.filepath, "r") as f:
            return json.load(f)

    def _write(self, messages):
        with open(self.filepath, "w") as f:
            json.dump(messages, f, indent=2)

    def append(self, message):
        """Persist a new message to disk."""
        messages = self._read()
        messages.append(message)
        self._write(messages)

    def get(self, message_id):
        """Recover a single message from disk by id. This is the method
        that proves a forwarding step actually depends on what's on disk,
        not just on whatever copy still happens to be sitting in RAM."""
        for m in self._read():
            if m["id"] == message_id:
                return m
        return None

    def remove(self, message_id):
        """Delete a message from disk once its delivery is confirmed."""
        messages = [m for m in self._read() if m["id"] != message_id]
        self._write(messages)

    def all(self):
        """Return every message currently persisted (mostly for debugging)."""
        return self._read()
