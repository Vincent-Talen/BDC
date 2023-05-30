#!/usr/bin/env python3

"""Script that calculates the mean PHRED scores per base position for FastQ files.

To decrease the runtime, the load is split between multiple computers using
the multiprocessing Process and Queue classes.

Starting a server:

Starting a client:
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "0.1"

# IMPORTS
import argparse
import multiprocessing
import queue
import time
from multiprocessing.managers import BaseManager

from pathlib import Path

import numpy as np

# GLOBALS
POISON_PILL = "NAWWSTAHPIT"
ERROR = "POEPIE"
AUTHKEY = b"yeahthisissecretdidyoureallythinkiwouldtellyou?"


# FUNCTIONS
def parse_args():
    """Parses the arguments given to the script.

    Returns:
        args: The parsed arguments.
    """
    # Initialize the parser
    parser = argparse.ArgumentParser(
        description="Script for Assignment 2 of the Big Data Computing course."
    )

    # Create the mutually exclusive group for the mode
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "-s",
        action="store_true",
        help="Run the program in Server mode; see extra options needed below"
    )
    mode.add_argument(
        "-c",
        action="store_true",
        help="Run the program in Client mode; see extra options needed below"
    )

    # Create group with server mode arguments
    server_args = parser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument(
        "--chunks",
        action="store",
        type=int,
        required=True
    )
    server_args.add_argument(
        "-o",
        action="store",
        dest="csvfile",
        type=Path,
        required=False,
        help="CSV file output should be saved to. Default is to write output to STDOUT."
    )
    server_args.add_argument(
        "fastq_files",
        action="store",
        type=Path,
        nargs="+",
        help="At least 1 Illumina FastQ Format file to process."
    )

    # Create group with client mode arguments
    client_args = parser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument(
        "-n",
        action="store",
        dest="n",
        type=int,
        required=False,
        help="Amount of cores to use per host."
    )
    client_args.add_argument(
        "--host",
        action="store",
        type=str,
        help="The hostname where the Server is listening"
    )
    client_args.add_argument(
        "--port",
        action="store",
        type=int,
        help="The port on which the Server is listening"
    )
    return parser.parse_args()


def non_existent(dingus):
    print("This function does not exist! But this is the Path:", dingus)


# CLASSES
class Server(multiprocessing.Process):
    def __init__(
        self,
        *,
        data,
        target_fun,
        host: str,
        port: str,
        outfile: Path | None = None
    ):
        super().__init__()
        self.data = data
        self.target_fun = target_fun
        self.host: str = host
        self.port: str = port
        self.outfile: Path | None = outfile

    def run(self):
        # Start a shared manager server and access its queues
        manager = self.__create_manager()
        shared_job_queue = manager.get_job_queue()
        shared_result_queue = manager.get_result_queue()

        # Check if there was even data supplied, otherwise just stop the server
        if not self.data:
            print("Server has nothing to do!")
            return

        # Put the data in the job queue
        print("Sending data!")
        for chunk in self.data:
            shared_job_queue.put(
                {"function": self.target_fun, "args": (chunk)}
            )
        time.sleep(2)

        # Get the results from the result queue
        results = self.__wait_and_get_results(shared_result_queue)

        # Tell the client process no more data will be forthcoming
        print("Time to kill some peons!")
        shared_job_queue.put(POISON_PILL)
        # Sleep 5 seconds to give connected clients the chance to properly shut down
        time.sleep(5)
        print("Server finished")
        manager.shutdown()
        print(results)

    def __create_manager(self):
        # Initialize the job and result queues
        job_queue = queue.Queue()
        result_queue = queue.Queue()

        # Create a custom manager class and register the queues
        class ServerSideQueueManager(BaseManager):
            pass

        ServerSideQueueManager.register(
            "get_job_queue", callable=lambda: job_queue
        )
        ServerSideQueueManager.register(
            "get_result_queue", callable=lambda: result_queue
        )

        # Create instance of the custom manager class and start it
        manager = ServerSideQueueManager(
            address=(self.host, self.port), authkey=AUTHKEY
        )
        manager.start()
        print(f"Server started at {self.host}:{self.port}")
        return manager

    def __wait_and_get_results(self, shared_result_queue: queue.Queue):
        results = list()
        while True:
            try:
                # Checks if there is data in the results queue, throws exception if not
                result = shared_result_queue.get_nowait()
                # If no exception is thrown there is data in the queue
                results.append(result)
                print("Got result!")

                # When all data has been processed, stop the loop waiting for results
                if len(results) == len(self.data):
                    print("Got all results!")
                    break
            except queue.Empty:
                time.sleep(1)
                continue
        return results


class Peon(multiprocessing.Process):
    def __init__(self, job_queue: queue.Queue, result_queue: queue.Queue):
        super().__init__()
        self.job_queue = job_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            try:
                job = self.job_queue.get_nowait()
                if job == POISON_PILL:
                    self.job_queue.put(POISON_PILL)
                    print("Aaaaaaargh", self.name)
                    return
                else:
                    try:
                        target_fun = job["function"]
                        arguments = job["args"]
                        print(f"Peon {self.name} Workwork on {arguments}!")
                        result = target_fun(arguments)
                        self.result_queue.put(
                            {"job": job, "result": result}
                        )
                    except NameError:
                        print("Can't find yer function Bob!")
                        self.result_queue.put(
                            {"job": job, "result": ERROR}
                        )

            except queue.Empty:
                print("sleepytime for", self.name)
                time.sleep(1)


class Client(multiprocessing.Process):
    def __init__(
        self,
        *,
        host: str,
        port: str,
        n: int,
    ):
        super().__init__()
        self.host: str = host
        self.port: str = port
        self.n: int = n

    def run(self):
        # Start a shared manager server and access its queues
        manager = self.__create_manager()
        job_queue = manager.get_job_queue()
        result_queue = manager.get_result_queue()

        # Start the workers
        self.__run_workers(job_queue, result_queue)

    def __create_manager(self):
        # Create a custom manager class and register the queues
        class ClientSideQueueManager(BaseManager):
            pass

        ClientSideQueueManager.register("get_job_queue")
        ClientSideQueueManager.register("get_result_queue")

        # Create instance of the custom manager class and connect to it
        manager = ClientSideQueueManager(
            address=(self.host, self.port), authkey=AUTHKEY
        )
        manager.connect()

        print(f"Client connected to {self.host}:{self.port}")
        return manager

    def __run_workers(self, job_queue: queue.Queue, result_queue: queue.Queue):
        processes = list()

        # Start the workers
        for _ in range(self.n):
            worker = Peon(
                job_queue=job_queue,
                result_queue=result_queue
            )
            processes.append(worker)
            worker.start()
        print(f"Started {self.n} workers!")

        # Wait for the workers to finish
        for process in processes:
            process.join()


# MAIN
def main():
    """Main function of the script."""
    # Parse arguments
    args = parse_args()

    # Checks if script is started as server or client mode
    if args.s:
        # Server mode
        data_idk = args.fastq_files[0]
        server = Server(
            data=data_idk,
            target_fun=non_existent,
            host=args.host,
            port=args.port,
            outfile=args.output_file
        )
        server.start()
        time.sleep(1)
    elif args.c:
        # Client mode
        client = Client(
            host=args.host,
            port=args.port,
            n=args.n
        )
        client.start()
        client.join()


if __name__ == "__main__":
    main()
