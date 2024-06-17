#!/usr/bin/env python3

"""Script that calculates the mean PHRED scores per base position for FastQ files.

It uses the assignment1 module in combination with MPI (mpi4py) and should only be run
on a cluster with MPI installed.

Usage:
    $ mpiexec -np <amount_of_processes> python3 assignment4.py
        fastq_file1.fastq [fastq_file2.fastq ... fastq_fileN.fastq]
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "0.3"

# IMPORTS
import argparse
import sys

from pathlib import Path
from mpi4py import MPI

ASSIGNMENT1_DIR_PATH = str(Path(__file__).parent.parent.joinpath("Assignment1"))
sys.path.append(ASSIGNMENT1_DIR_PATH)
from assignment1 import FastQFileHandler


# FUNCTIONS
def parse_cli_args() -> argparse.Namespace:
    """Parses the CLI arguments given to the script.

    Returns:
        The parsed arguments as a Namespace.
    """
    parser = argparse.ArgumentParser(
        description="Script for Assignment 4 of the Big Data Computing course."
    )
    parser.add_argument(
        "-o",
        action="store",
        dest="output_file",
        type=Path,
        required=False,
        help="CSV file output should be saved to. Default is to write output to STDOUT."
    )
    parser.add_argument(
        "fastq_files",
        action="store",
        type=Path,
        nargs="+",
        help="At least 1 Illumina FastQ Format file to process."
    )
    return parser.parse_args()


def main():
    """Main function of the script."""
    # Parse arguments
    args = parse_cli_args()

    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    nproc = comm.Get_size()

    if rank == 0:
        # Create file handler and use it to generate chunks
        file_handler = FastQFileHandler(
            fastq_files=args.fastq_files,
            output_file=args.output_file,
            chunk_count=nproc,
        )
        unprocessed_chunks = list(file_handler.generate_chunks())
    else:
        unprocessed_chunks = None

    # The controller scatters chunks to the workers but also keeps one to process itself
    # Each worker gets a chunk and then all processes will process their assigned chunk
    chunk_object = comm.scatter(unprocessed_chunks, root=0)
    processed_chunk = chunk_object.perform_calculations()

    # Each process sends the processed chunk and the master gathers them all back
    all_processed_chunks = comm.gather(processed_chunk, root=0)

    # The controller should finalize the script by further processing the results
    if rank == 0:
        file_handler.process_results(all_processed_chunks)


if __name__ == "__main__":
    main()
