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
from assignment1 import process_numpy_arrays


# FUNCTIONS
def parse_cli_args() -> argparse.Namespace:
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
        "-o",
        action="store",
        dest="output_file",
        type=Path,
        required=False,
        help="CSV file output should be saved to. Default is to write output to STDOUT."
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
    args = parse_cli_args()
    # Empty sys.argv so fileinput.input() will read from STDIN
    sys.argv = sys.argv[0:1]

    # Checks if script is started as chunk or combine mode
    if args.chunk:
        # Get the quality lines as ascii unsigned integers in numpy arrays
        quality_array_list = [
            np.frombuffer(line, dtype=np.uint8)
            for line in quality_line_generator()
        ]

        # Get the total phred score and the amount of characters per position/column
        sum_array, count_array = process_numpy_arrays(quality_array_list, phred=True)
        print("sum:", list(sum_array))
        print("count:", list(count_array))
    elif args.combine:
        sum_arrays = []
        count_arrays = []
        with fileinput.input(mode="r") as file:
            for line in file:
                if line.startswith("sum:"):
                    sum_arrays.append(
                        np.fromstring(line.strip()[6:-1], sep=", ", dtype=np.int64)
                    )
                elif line.startswith("count:"):
                    count_arrays.append(
                        np.fromstring(line.strip()[8:-1], sep=", ", dtype=np.int64)
                    )

        # Get the total sum and position counts of the file
        total_sum, _ = process_numpy_arrays(sum_arrays)
        total_counts, _ = process_numpy_arrays(count_arrays)

        # Calculate the total average phred score per position for the file
        file_phred_averages = np.divide(total_sum, total_counts, dtype=np.float64)

        # Open the output file if specified, otherwise use sys.stdout
        out_target = args.output_file.open("w") if args.output_file else sys.stdout

        with out_target as output:
            for i, pos in enumerate(file_phred_averages):
                output.write(f"{i},{pos}\n")


if __name__ == "__main__":
    main()
