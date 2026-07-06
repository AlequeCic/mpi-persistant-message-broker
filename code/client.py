"""
Chat client (any rank != 0) representing one chat user.

Each user has a small scripted list of messages to send to the other
user (routed through the server), sent at random points in time across
the simulation. At the same time, every user continuously polls for
messages addressed to them.
"""
import random
import time
import uuid

from mpi4py import MPI

from persistent_queue import PersistentQueue

TAG_DATA = 11
TAG_ACK = 12


def run_chat_client(
    comm,
    rank,
    name,
    destination_rank,
    total_iterations,
    sleep_range,
    messages_to_send=None,
    offline_at_iteration=None,
    offline_duration=0,
):
    """
    name:                 display name for this user, e.g. "Alice".
    destination_rank:     rank of the other chat user.
    messages_to_send:     list of strings this user will send, one at a
                          time, at random iterations spread across the run.
    offline_at_iteration: if set, instead of the normal short random sleep,
                          this user sleeps for offline_duration seconds at
                          that iteration, simulating a real disconnection.
                          Messages sent to it by the other user must still
                          arrive once it wakes up (that's the whole point
                          of the chaos demo).
    """
    # Two separate persistent queues instead of one shared file:
    #   outbox -> messages THIS user originated, removed once the server
    #             confirms delivery (ACK received)
    #   inbox  -> messages THIS user received, kept as a permanent record
    #             (this is the user's "mailbox")
    outbox = PersistentQueue(f"outbox_{name}.json")
    inbox = PersistentQueue(f"inbox_{name}.json")
    pending_requests = []
    messages_to_send = list(messages_to_send or [])
    sleep_min, sleep_max = sleep_range
    
    # --- crash recovery: anything still sitting in the outbox from a
    # previous run was persisted but never got its ACK back, i.e. the
    # server never confirmed it arrived. Resend it now, straight from
    # disk. The server deduplicates by id, so resending something that
    # actually did get through before is harmless. ---
    leftover_messages = outbox.all()
    if leftover_messages:
        print(f"[{name}] recovering {len(leftover_messages)} pending message(s) from outbox_{name}.json", flush=True)
        for msg in leftover_messages:
            print(f"[{name}] resending recovered message {msg['id'][:8]}: \"{msg['payload']}\"", flush=True)
            req = comm.isend(msg, dest=0, tag=TAG_DATA)
            pending_requests.append(req)

    # decide in advance which iterations will fire a send, so messages are
    # spread out across the run instead of all sent immediately.
    send_at_iterations = []
    if messages_to_send:
        candidate_iterations = range(1, total_iterations)
        k = min(len(messages_to_send), len(candidate_iterations))
        send_at_iterations = sorted(random.sample(candidate_iterations, k=k))
 
    for iteration in range(1, total_iterations + 1):
        if iteration == offline_at_iteration:
            print(f"[{name}] going offline for {offline_duration}s (simulating a dropped connection)...", flush=True)
            time.sleep(offline_duration)
            print(f"[{name}] back online, catching up on messages", flush=True)
        else:
            time.sleep(random.uniform(sleep_min, sleep_max))
 
        pending_requests = [r for r in pending_requests if not r.test()[0]]
 
        # --- send a scripted message if this is its scheduled turn ---
        if send_at_iterations and iteration == send_at_iterations[0]:
            send_at_iterations.pop(0)
            text = messages_to_send.pop(0)
            message = {
                "id": str(uuid.uuid4()),
                "type": "data",
                "origem": rank,
                "destino": destination_rank,
                "payload": text,
                "sender_name": name,
                "timestamp": time.time(),
            }
            outbox.append(message)
            print(f"[{name}] sending: \"{text}\"", flush=True)
            req = comm.isend(message, dest=0, tag=TAG_DATA)
            pending_requests.append(req)
 
        # --- receive messages addressed to this user ---
        while comm.iprobe(source=0, tag=TAG_DATA):
            msg = comm.recv(source=0, tag=TAG_DATA)
            inbox.append(msg)
            print(f"[{name}] received from {msg['sender_name']}: \"{msg['payload']}\"", flush=True)
            ack = {"id": msg["id"], "type": "ack"}
            req = comm.isend(ack, dest=0, tag=TAG_ACK)
            pending_requests.append(req)
 
        # --- confirm delivery of messages this user sent earlier ---
        while comm.iprobe(source=0, tag=TAG_ACK):
            ack = comm.recv(source=0, tag=TAG_ACK)
            outbox.remove(ack["id"])
            print(f"[{name}] message {ack['id'][:8]} delivered (removed from local queue)", flush=True)
 
    MPI.Request.waitall(pending_requests)
    print(f"[{name}] chat session ended", flush=True)
