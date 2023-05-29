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
import multiprocessing as mp

from pathlib import Path

import numpy as np

# GLOBALS


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


def main():
    """Main function of the script."""
    # Parse arguments
    args = parse_args()

    # Checks if script is started as server or client mode
    if args.s:
        # Server mode
        print("Server mode")
        print(args)
    elif args.c:
        # Client mode
        print("Client mode")
        print(args)


if __name__ == "__main__":
    main()
