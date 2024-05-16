#!/usr/bin/env python3

"""Script that calculates the mean PHRED scores per base position for FastQ files.

To decrease the runtime, the load is split between multiple computers using
the multiprocessing Process and Queue classes.

Starting a server:
    $ python assignment2.py
        -s
        --host localhost
        --port 25715
        --chunks 4
        -o output.csv

Starting a client:
    $ python assignment2.py
        -c
        --host localhost
        --port 25715
        -n 4
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "1.2"

# IMPORTS
import argparse
import multiprocessing as mp
import queue
import sys
import time

from multiprocessing.managers import BaseManager
from pathlib import Path

ASSIGNMENT1_DIR_PATH = str(Path(__file__).parent.parent.joinpath("Assignment1"))
sys.path.append(ASSIGNMENT1_DIR_PATH)
from assignment1 import FastQChunk, FastQFileHandler

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

    # Add socket options
    parser.add_argument(
        "--host",
        action="store",
        type=str,
        required=True,
        help="The hostname where the Server is listening",
    )
    parser.add_argument(
        "--port",
        action="store",
        type=int,
        required=True,
        help="The port on which the Server is listening",
    )

    # Create the mutually exclusive group for the mode
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "-s",
        action="store_true",
        help="Run the program in Server mode; see extra options needed below",
    )
    mode.add_argument(
        "-c",
        action="store_true",
        help="Run the program in Client mode; see extra options needed below",
    )

    # Create group with server mode arguments
    server_args = parser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument(
        "--chunks",
        action="store",
        type=int,
        required=False,
        default=20,
        help="Amount of chunks to split the data into.",
    )
    server_args.add_argument(
        "-o",
        action="store",
        dest="output_file",
        type=Path,
        required=False,
        help="File output should be saved to. Default is to write output to STDOUT.",
    )
    server_args.add_argument(
        "fastq_files",
        action="store",
        type=Path,
        nargs="*",
        help="At least 1 Illumina FastQ Format file to process.",
    )

    # Create group with client mode arguments
    client_args = parser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument(
        "-n",
        action="store",
        dest="core_count",
        type=int,
        required=False,
        default=4,
        help="Amount of cores to use per client.",
    )
    return parser.parse_args()


# CLASSES
class Server(mp.Process):
    """Splits the data into chunks and starts socket client can connect to for the jobs.

    Attributes:
        file_handler (FastQFileHandler):
            A file handler instance with the fastq data to be processed.
        target_fun:
            The function to be executed on the data.
        host (str):
            The hostname the Server will open the socket at.
        port (str):
            The port the Server will open the socket at.
        outfile (Path | None):
            The file output should be saved to.
    """
    def __init__(
        self,
        *,
        file_handler: FastQFileHandler,
        target_fun,
        host: str,
        port: str,
        outfile: Path | None = None
    ):
        """Initializes the Server object.

        Args:
            file_handler (FastQFileHandler):
                A file handler instance with the fastq data to be processed.
            target_fun:
                The function to be executed on the data.
            host (str):
                The hostname the Server will open the socket at.
            port (str):
                The port the Server will open the socket at.
            outfile (Path | None):
                The file output should be saved to.
        """
        super().__init__()
        self.file_handler = file_handler
        self.target_fun = target_fun
        self.host: str = host
        self.port: str = port
        self.outfile: Path | None = outfile

    def run(self):
        """Starts the server process, creates manager and fills the job queue with data.

        This method is called when the process is started with the start() method,
        there is thus no need to call this method yourself directly.

        It will keep the server and manager running until all jobs have been returned.
        """
        # Use the file handler to generate the chunks to be processed
        unprocessed_chunks = self.file_handler.chunk_generator()

        # Start a shared manager server and access its queues
        with self.__create_manager() as manager:
            shared_job_queue = manager.get_job_queue()
            shared_result_queue = manager.get_result_queue()

            # Put the chunks in the job queue
            print("Sending data!")
            for chunk in unprocessed_chunks:
                shared_job_queue.put({"function": self.target_fun, "chunk_obj": chunk})
            time.sleep(2)

            # Get the results from the result queue
            print("Now waiting for results!")
            job_results = self.__wait_and_get_results(shared_result_queue)

            # Tell the client process no more data will be forthcoming
            print("Time to kill some peons!")
            shared_job_queue.put(POISON_PILL)
            # Sleep 5 seconds to give connected clients the chance to properly shut down
            time.sleep(5)
            print("Server finished")

        # Finalize by further processing the results
        processed_chunks = (job_dict["result"] for job_dict in job_results)
        self.file_handler.process_results(processed_chunks)

    def __create_manager(self):
        """Creates and starts manager with the job and result queues on the socket."""
        # Initialize the job and result queues
        job_queue = queue.Queue()
        result_queue = queue.Queue()

        # Create a custom manager class and register the queues
        class ServerSideManager(BaseManager):
            """Custom manager class for hosting the job and result queues."""

        ServerSideManager.register("get_job_queue", callable=lambda: job_queue)
        ServerSideManager.register("get_result_queue", callable=lambda: result_queue)

        # Create instance of the custom manager class and start it
        manager = ServerSideManager(address=(self.host, self.port), authkey=AUTHKEY)
        manager.start()

        print(f"Server started at {self.host}:{self.port}")
        return manager

    def __wait_and_get_results(self, shared_result_queue: queue.Queue):
        """Waits for results to be put in the result queue and returns them.

        It keeps checking the result queue until all results have been returned.

        Args:
            shared_result_queue (queue.Queue):
                The queue to get the results from.
        """
        results = []
        while True:
            try:
                # Checks if there is data in the results queue, throws exception if not
                result = shared_result_queue.get_nowait()
                # If no exception is thrown there is data in the queue
                results.append(result)
                print("Got a result!")

                # When all data has been processed, stop the loop waiting for results
                if len(results) == self.file_handler.chunk_count:
                    print("Got all results!")
                    break
            except queue.Empty:
                time.sleep(1)
                continue
        return results


class Peon(mp.Process):
    """Gets jobs from the job queue and executes them.

    Attributes:
        job_queue (queue.Queue):
            The queue to get the jobs from.
        result_queue (queue.Queue):
            The queue to put the results in.
    """
    def __init__(self, job_queue: queue.Queue, result_queue: queue.Queue):
        """Initializes the Peon object.

        Args:
            job_queue (queue.Queue):
                The queue to get the jobs from.
            result_queue (queue.Queue):
                The queue to put the results in.
        """
        super().__init__()
        self.job_queue = job_queue
        self.result_queue = result_queue

    def run(self):
        """Starts the peon process, gets jobs from the job queue and executes them.

        This method is called when the process is started with the start() method,
        there is thus no need to call this method yourself directly.
        """
        while True:
            try:
                # Try to get a job from the job queue
                job = self.job_queue.get_nowait()

                # If the job is the poison pill use return statement, killing this peon
                if job == POISON_PILL:
                    # Place the poison pill back into the queue so other peons also die
                    self.job_queue.put(POISON_PILL)
                    print("Aaaaaaargh", self.name)
                    return

                # Job received, try executing it
                try:
                    target_fun = job["function"]
                    arguments = job["chunk_obj"]
                    print(f"Peon {self.name} Workwork on {arguments}!")
                    result = target_fun(arguments)
                    self.result_queue.put({"job": job, "result": result})
                except NameError:
                    print("Target function not found!")
                    self.result_queue.put({"job": job, "result": ERROR})
            except queue.Empty:
                # No jobs found in job queue
                print("sleepytime for", self.name)
                time.sleep(1)


class Client(mp.Process):
    """Creates a client process that connects to the manager server and starts workers.

    Attributes:
        host (str):
            The host address of the manager server.
        port (str):
            The port of the manager server.
        core_count (int):
            The number of workers to start.
    """
    def __init__(
        self,
        *,
        host: str,
        port: str,
        core_count: int,
    ):
        """Initializes the Client object.

        Args:
            host (str):
                The host address of the manager server.
            port (str):
                The port of the manager server.
            core_count (int):
                The number of workers to start.
        """
        super().__init__()
        self.host: str = host
        self.port: str = port
        self.core_count: int = core_count

    def run(self):
        """Starts the client process, connects to the manager server and starts workers.

        This method is called when the process is started with the start() method,
        there is thus no need to call this method yourself directly.
        """
        # Connect to the manager server and access its queues
        manager = self.__create_manager()
        job_queue = manager.get_job_queue()
        result_queue = manager.get_result_queue()

        # Start the workers
        self.__run_workers(job_queue, result_queue)

    def __create_manager(self):
        """Creates and connects a client manager to the manager server's socket."""
        # Create a custom manager class and register the queues
        class ClientSideManager(BaseManager):
            """Custom manager class that connects to the manager server."""

        ClientSideManager.register("get_job_queue")
        ClientSideManager.register("get_result_queue")

        # Create instance of the custom manager and connect to the server with it
        manager = ClientSideManager(address=(self.host, self.port), authkey=AUTHKEY)
        manager.connect()

        print(f"Client connected to {self.host}:{self.port}")
        return manager

    def __run_workers(self, job_queue: queue.Queue, result_queue: queue.Queue):
        """Starts worker peons and waits for them to finish.

        Args:
            job_queue (queue.Queue):
                The queue to get the jobs from.
            result_queue (queue.Queue):
                The queue to put the results in.
        """
        processes = []

        # Start the workers
        for _ in range(self.core_count):
            worker = Peon(job_queue=job_queue, result_queue=result_queue)
            processes.append(worker)
            worker.start()
        print(f"Started {self.core_count} worker peons!")

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
        # Create file handler instance
        file_handler = FastQFileHandler(
            fastq_files=args.fastq_files,
            output_file=args.output_file,
            chunk_count=args.chunks,
            min_chunk_size=1024
        )

        # Start the server
        server = Server(
            file_handler=file_handler,
            target_fun=FastQChunk.perform_stuff,
            host=args.host,
            port=args.port,
            outfile=args.output_file,
        )
        server.start()
        time.sleep(1)
    elif args.c:
        # Client mode
        client = Client(host=args.host, port=args.port, core_count=args.core_count)
        client.start()
        client.join()


if __name__ == "__main__":
    main()
