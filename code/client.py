"""
Client process (any rank != 0).

A client can act as:
  - an ORIGIN: creates one message, persists it locally, sends it to the
    server, and waits for an ACK before deleting it from its own queue.
  - a DESTINATION: receives messages routed by the server, persists them
    locally, and sends an ACK back to the server.

Both roles share the same loop below; a single client can be origin and
destination at the same time (pass destination_rank=None if this client
should not originate anything).
"""
import random
import sys
import time
import uuid

from mpi4py import MPI

from persistent_queue import PersistentQueue

TAG_DATA = 11
TAG_ACK = 12

SLEEP_MIN, SLEEP_MAX = 0.1, 1.5  # seconds


def run_client(comm, rank, total_iterations, destination_rank=None, payload=None):
    queue = PersistentQueue(f"queue_{rank}.json")
    pending_requests = []
    already_sent = destination_rank is None  # nothing to originate -> skip

    for _ in range(total_iterations):
        # Simulate local processing time / connection drops.
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

        pending_requests = [r for r in pending_requests if not r.test()[0]]

        # --- Origin behaviour: create and send exactly one message ---
        if not already_sent:
            message = {
                "id": str(uuid.uuid4()),
                "type": "data",
                "origem": rank,
                "destino": destination_rank,
                "payload": payload,
                "timestamp": time.time(),
            }
            queue.append(message)
            print(f"[client {rank}] created {message['id']} for rank {destination_rank}", flush=True)

            # Non-blocking send: the client can go back to sleep right away,
            # it does not wait for the server to wake up.
            req = comm.isend(message, dest=0, tag=TAG_DATA)
            pending_requests.append(req)
            already_sent = True

        # --- Destination behaviour: poll for incoming data from the server ---
        while comm.iprobe(source=0, tag=TAG_DATA):
            msg = comm.recv(source=0, tag=TAG_DATA)
            queue.append(msg)
            print(f"[client {rank}] received {msg['id']} from rank {msg['origem']}: {msg['payload']!r}", flush=True)

            ack = {"id": msg["id"], "type": "ack"}
            req = comm.isend(ack, dest=0, tag=TAG_ACK)
            pending_requests.append(req)

        # --- Origin behaviour: poll for the server's ACK, then clean up ---
        while comm.iprobe(source=0, tag=TAG_ACK):
            ack = comm.recv(source=0, tag=TAG_ACK)
            queue.remove(ack["id"])
            print(f"[client {rank}] {ack['id']} confirmed by server, removed locally", flush=True)

    MPI.Request.waitall(pending_requests)
    print(f"[client {rank}] finished, all pending sends flushed", flush=True)
