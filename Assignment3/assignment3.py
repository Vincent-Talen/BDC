#!/usr/bin/env python3

"""Script that calculates the mean PHRED scores per base position for FastQ files.

The script expects STDIN to have lines!

Usage:
    $ python3 assignment3.py
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "0.1"

# IMPORTS
import fileinput

import numpy as np


# FUNCTIONS
def combine_numpy_arrays(array_list: list[np.ndarray]):
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
    print("\t", sum_array)
    print("\t", position_count_array)


if __name__ == "__main__":
    main()
