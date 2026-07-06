"""
Chat demo — normal flow.

Rank 0 -> server (broker)
Rank 1 -> Alice
Rank 2 -> Bob

Alice and Bob exchange a handful of messages through the server, each
sleeping for a random (but short) interval between actions. This is the
"everything works as expected" demonstration.

Run with:
    mpirun --oversubscribe -n 3 python3 main_chat_normal.py
"""
import sys

from mpi4py import MPI

from client import run_chat_client
from server import run_server

TOTAL_ITERATIONS = int(sys.argv[1]) if len(sys.argv) > 1 else 35
NORMAL_SLEEP_RANGE = (0.2, 2.8)

ALICE_MESSAGES = [
    "Oi Bob, tudo bem?",
    "Vi que voce terminou o trabalho de SD, parabens!",
    "Bora almocar mais tarde?",
]

BOB_MESSAGES = [
    "Oi Alice! Tudo certo por aqui.",
    "Ainda estou revisando o codigo, mas ta quase la.",
    "Bora sim, me chama por volta de meio-dia.",
]

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        run_server(comm, TOTAL_ITERATIONS, sleep_range=NORMAL_SLEEP_RANGE)
    elif rank == 1:
        run_chat_client(
            comm, rank, name="Alice", destination_rank=2,
            total_iterations=TOTAL_ITERATIONS, sleep_range=NORMAL_SLEEP_RANGE,
            messages_to_send=ALICE_MESSAGES,
        )
    elif rank == 2:
        run_chat_client(
            comm, rank, name="Bob", destination_rank=1,
            total_iterations=TOTAL_ITERATIONS, sleep_range=NORMAL_SLEEP_RANGE,
            messages_to_send=BOB_MESSAGES,
        )
