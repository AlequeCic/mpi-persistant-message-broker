"""
Server process (rank 0) - acts as the central broker.

Responsibilities, executed every loop iteration:
  1. Sleep for a random interval (simulates unpredictable availability;
     this must never cause a deadlock elsewhere in the system).
  2. Poll (Iprobe) for DATA messages from any client.
  3. Persist each new message to its own queue file (fs), deduplicating
     by message id so a lost ACK/retry doesn't create a second delivery.
  4. Route the message to its final destination with a non-blocking send.
  5. Send an ACK back to the original sender.
  6. Poll for ACKs coming back from final destinations and remove the
     corresponding message from fs (transaction complete).
"""
import random
import sys
import time

from mpi4py import MPI

from persistent_queue import PersistentQueue

TAG_DATA = 11
TAG_ACK = 12

SLEEP_MIN, SLEEP_MAX = 0.1, 1.0  # seconds


def run_server(comm, total_iterations, sleep_range=(SLEEP_MIN, SLEEP_MAX)):
    queue = PersistentQueue("queue_server.json")
    processed_ids = set()       # for deduplication (closer to exactly-once)
    pending_requests = []       # Request objects from isend(), must be tracked
    sleep_min, sleep_max = sleep_range

    # --- Crash recovery: anything still sitting in fs from a previous run
    # was persisted but never got its ACK back, i.e. delivery was never
    # confirmed. Re-forward it now, straight from disk. This is what makes
    # the file the actual source of truth, not just a log nobody reads. ---
    leftover_messages = queue.all()
    if leftover_messages:
        print(f"[server] recovering {len(leftover_messages)} pending message(s) from disk (queue_server.json)", flush=True)
        for msg in leftover_messages:
            processed_ids.add(msg["id"])
            print(f"[server] re-forwarding recovered message {msg['id'][:8]} -> rank {msg['destino']}", flush=True)
            req = comm.isend(msg, dest=msg["destino"], tag=TAG_DATA)
            pending_requests.append(req)

    for _ in range(total_iterations):
        # Simulate the server being busy / unavailable at random times.
        time.sleep(random.uniform(sleep_min, sleep_max))

        # Drop send requests that have already completed.
        pending_requests = [r for r in pending_requests if not r.test()[0]]

        # --- Handle every DATA message currently waiting from any client ---
        while comm.iprobe(source=MPI.ANY_SOURCE, tag=TAG_DATA):
            status = MPI.Status()
            msg = comm.recv(source=MPI.ANY_SOURCE, tag=TAG_DATA, status=status)
            sender_rank = status.Get_source()

            if msg["id"] not in processed_ids:
                processed_ids.add(msg["id"])
                queue.append(msg)

                # Recover the message FROM THE FILE before forwarding it.
                # This is the step that makes persistence real rather than
                # cosmetic: the data actually being sent onward is proven
                # to have survived a disk round-trip, not just whatever
                # copy still happens to be sitting in this process's RAM.
                persisted_msg = queue.get(msg["id"])

                preview = persisted_msg.get("payload", "")
                sender_label = persisted_msg.get("sender_name", f"rank {sender_rank}")
                print(f"[server] routing message from {sender_label} -> rank {persisted_msg['destino']}: \"{preview}\"", flush=True)

                # Forward to the final destination. Non-blocking: if the
                # destination is asleep, this call still returns immediately.
                req = comm.isend(persisted_msg, dest=persisted_msg["destino"], tag=TAG_DATA)
                pending_requests.append(req)
            else:
                print(f"[server] duplicate {msg['id']} ignored (already processed)", flush=True)

            # Acknowledge receipt to the original sender either way, so the
            # sender can safely delete the message from its own queue.
            ack = {"id": msg["id"], "type": "ack"}
            req_ack = comm.isend(ack, dest=sender_rank, tag=TAG_ACK)
            pending_requests.append(req_ack)

        # --- Handle every ACK coming back from final destinations ---
        while comm.iprobe(source=MPI.ANY_SOURCE, tag=TAG_ACK):
            ack = comm.recv(source=MPI.ANY_SOURCE, tag=TAG_ACK)
            queue.remove(ack["id"])
            print(f"[server] delivery of {ack['id']} confirmed, removed from fs", flush=True)

    # Make sure every asynchronous send actually left the network before exiting.
    MPI.Request.waitall(pending_requests)
    print("[server] finished, all pending sends flushed", flush=True)
