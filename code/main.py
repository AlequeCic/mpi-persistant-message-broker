"""
Entry point for the whole simulation.

mpirun launches this exact same script on every process; MPI.COMM_WORLD's
rank is what tells each process which role to play:

    rank 0            -> server (broker)
    rank 1             -> origin client (creates and sends one message)
    rank 2, 3, ...     -> destination clients (only receive)

Run with, e.g.:
    mpirun --oversubscribe -n 3 python3 main.py 30

The trailing "30" is the number of loop iterations each process runs
before finishing (default 30). Increase it if messages arrive late
because of long random sleeps.
"""
import sys

from mpi4py import MPI

from client import run_client
from server import run_server

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    iterations = int(sys.argv[1]) if len(sys.argv) > 1 else 30

    if rank == 0:
        run_server(comm, iterations)
    elif rank == 1:
        run_client(comm, rank, iterations, destination_rank=2, payload="Hello from client 1!")
    else:
        run_client(comm, rank, iterations)
