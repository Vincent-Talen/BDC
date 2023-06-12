#!/usr/bin/env python3

"""Script that calculates the mean PHRED scores per base position for FastQ files.

The script expects STDIN to have lines!

Usage:
    $ python3 assignment3.py
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "0.2"

# IMPORTS
import argparse
import fileinput
import sys

from pathlib import Path

import numpy as np


# FUNCTIONS
def parse_args():
    """Parses the arguments given to the script.

    Returns:
        args: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Script for Assignment 3 of the Big Data Computing course."
    )
    parser.add_argument(
        "--filename",
        action="store",
        type=Path,
        required=False,
        help="The name of the fastq input file."
    )

    # Create the mutually exclusive group for the mode
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--chunk",
        action="store_true",
        help="Run the program in chunk mode; get the sum and count of the positions",
    )
    mode.add_argument(
        "--combine",
        action="store_true",
        help="Run the program in combine mode; calculating the total mean of a file",
    )
    return parser.parse_args()


def combine_numpy_arrays(array_list: list[np.ndarray], *, phred: bool = False):
    # Create array with the length of every line
    row_lengths = np.array([len(item) for item in array_list])
    # Create 2-D boolean array indicating if lines have a character at a position
    bool_array = row_lengths[:, None] > np.arange(row_lengths.max())
    # Create 2-D array containing zeros in the same shape as the boolean array
    complete_array = np.zeros(bool_array.shape, dtype=int)
    # Place the lines' phred scores (ascii-33) into the 2-D array
    complete_array[bool_array] = np.concatenate(array_list) - 33
    return complete_array


def quality_line_generator():
    with fileinput.input(mode="rb") as file:
        while header := file.readline():
            # Skip the sequence and separator line
            file.readline()
            file.readline()
            # Yield the quality line
            yield file.readline().strip()


def main():
    """Main function of the script."""
    # Parse arguments
    args = parse_args()
    # Empty sys.argv so fileinput.input() will read from STDIN
    sys.argv = sys.argv[0:1]

    # Checks if script is started as chunk or combine mode
    if args.chunk:
        # Get the quality lines as ascii unsigned integers in numpy arrays
        quality_array_list = [
            np.frombuffer(line, dtype=np.uint8)
            for line in quality_line_generator()
        ]

        # Create a single array containing all the quality lines' phred scores
        complete_phred_array = combine_numpy_arrays(quality_array_list)

        # Calculate the sum and count/weight of each column for the chunk
        sum_array = np.sum(complete_phred_array, axis=0)
        position_count_array = np.count_nonzero(complete_phred_array, axis=0)
        print("sum:", list(sum_array))
        print("count:", list(position_count_array))
    elif args.combine:
        print(str(args.filename))
        with fileinput.input(mode="r") as file:
            for line in file:
                print(line.strip())


if __name__ == "__main__":
    main()