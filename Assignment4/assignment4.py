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
__version__ = "0.1"

# IMPORTS
import argparse
import sys

from pathlib import Path
from mpi4py import MPI

ASSIGNMENT1_DIR_PATH = str(Path(__file__).parent.parent.joinpath("Assignment1"))
sys.path.append(ASSIGNMENT1_DIR_PATH)
from assignment1 import FastQFileHandler


# FUNCTIONS
def parse_args():
    """Parses the arguments given to the script.

    Returns:
        args: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Script for Assignment 1 of the Big Data Computing course."
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
    args = parse_args()

    # Initialize MPI
    comm = MPI.COMM_WORLD
    myrank = comm.Get_rank()
    nproc = comm.Get_size()

    if myrank == 0:
        # Create file handler and use it to generate chunks
        file_handler = FastQFileHandler(
            fastq_files=args.fastq_files,
            chunk_count=nproc,
            min_chunk_size=1
        )
        unprocessed_chunks = list(file_handler.chunk_generator())
    else:
        unprocessed_chunks = None

    # The master scatters the chunks to the workers but also keeps one to process itself
    # Each worker gets a chunk and then all processes will process their assigned chunk
    chunk_object = comm.scatter(unprocessed_chunks, root=0)
    processed_chunk = chunk_object.perform_stuff()

    # Each process sends the processed chunk and the master gathers them all back
    all_processed_chunks = comm.gather(processed_chunk, root=0)

    # The master should finalize the script by further processing the results
    if myrank == 0:
        file_handler.process_results(all_processed_chunks)


if __name__ == "__main__":
    main()
