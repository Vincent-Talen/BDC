#!/usr/bin/env python3

"""Script that calculates the mean PHRED scores per base position for FastQ files.

The script expects STDIN to have lines!

Examples:
    $ python3 assignment3.py --chunk

    $ python3 assignment3.py --combine --filename <fastq_file.fastq>
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "0.4"

# IMPORTS
import argparse
import fileinput
import sys

from pathlib import Path

import numpy as np

A1_DIR = str(Path(__file__).parent.parent.joinpath("Assignment1"))
sys.path.append(A1_DIR)
from assignment1 import combine_numpy_arrays


# FUNCTIONS
def parse_args() -> argparse.Namespace:
    """Parses the CLI arguments given to the script.

    Returns:
        The parsed arguments as a Namespace.
    """
    parser = argparse.ArgumentParser(
        description="Script for Assignment 3 of the Big Data Computing course."
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

    # Create group with combine mode arguments
    combine_args = parser.add_argument_group(title="Arguments when ran in combine mode")
    combine_args.add_argument(
        "--filename",
        action="store",
        type=Path,
        required=False,
        help="The name of the fastq input file."
    )
    return parser.parse_args()


def quality_line_generator() -> bytes:
    """Generator that only yields the quality lines of FastQ entries.

    Yields:
        The quality line of a FastQ entry as a bytes object.
    """
    with fileinput.input(mode="rb") as file:
        # For as long as there are header lines
        while file.readline():
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
        complete_phred_array = combine_numpy_arrays(quality_array_list, phred=True)

        # Calculate the sum and count/weight of each column for the chunk
        sum_array = np.sum(complete_phred_array, axis=0)
        position_count_array = np.count_nonzero(complete_phred_array, axis=0)
        print("sum:", list(sum_array))
        print("count:", list(position_count_array))
    elif args.combine:
        print(str(args.filename))
        sum_arrays = []
        count_arrays = []
        with fileinput.input(mode="r") as file:
            for line in file:
                if line.startswith("sum:"):
                    sum_arrays.append(np.fromstring(line.strip()[6:-1], sep=", "))
                elif line.startswith("count:"):
                    count_arrays.append(np.fromstring(line.strip()[8:-1], sep=", "))

        # Combine the sums and position counts of all the chunks of the file
        total_sum = np.sum(combine_numpy_arrays(sum_arrays), axis=0)
        total_counts = np.sum(combine_numpy_arrays(count_arrays), axis=0)

        # Calculate the total average phred score per position for the file
        file_phred_averages = np.divide(total_sum, total_counts, dtype=np.float64)
        for i, pos in enumerate(file_phred_averages):
            print(f"{i},{pos}")


if __name__ == "__main__":
    main()
