#!/usr/bin/env python3

"""Script that calculates the mean PHRED scores per base position for FastQ files.

To decrease the runtime, the script does this parallelized using multiprocessing.Pool.

Usage:
    $ python3 assignment1.py
        -n <cpu_count>
        [-o <output csv file>]
        fastq_file1.fastq [fastq_file2.fastq ... fastq_fileN.fastq]
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "2.0"

# IMPORTS
import argparse
import multiprocessing as mp
import sys

from pathlib import Path

import numpy as np

# GLOBALS


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
        "-n",
        action="store",
        dest="cpu_count",
        type=int,
        required=True,
        help="Amount of cores to use."
    )
    parser.add_argument(
        "-o",
        action="store",
        dest="csvfile",
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


# CLASSES
class FastQFileHandler:
    def __init__(self, fastq_files: list[Path], chunk_count: int):
        self.file_paths: list[Path] = fastq_files
        self.chunk_count: int = chunk_count
        self._check_if_files_exist()

    def _check_if_files_exist(self):
        for file_path in self.file_paths:
            if not file_path.exists():
                print(f"ERROR: File '{file_path}' does not exist! Exiting...")
                sys.exit(1)

    def create_chunk_dicts_for_files(self):
        dicts_list = []
        for filepath in self.file_paths:
            # Get the byte size of the current file
            file_byte_size = filepath.stat().st_size

            # Calculate chunk sizes and remainder of bytes
            quotient, remainder = divmod(file_byte_size, self.chunk_count)

            # Calculate the start and stop byte offsets for each chunk
            chunk_offsets = []
            for i in range(self.chunk_count):
                start = i * quotient + min(i, remainder)
                stop = (i + 1) * quotient + min(i + 1, remainder)
                chunk_offsets.append((start, stop))

            # Create a dictionary with the chunk information and the current filepath
            cur_file_dict = {"filepath": filepath, "chunk_offsets": chunk_offsets}
            dicts_list.append(cur_file_dict)
        return dicts_list


def main():
    """Main function of the script."""
    # Parse arguments
    args = parse_args()

    # File handler
    file_handler = FastQFileHandler(
        fastq_files=args.fastq_files, chunk_count=args.cpu_count
    )
    file_dicts_list = file_handler.create_chunk_dicts_for_files()
    print(file_dicts_list)


if __name__ == "__main__":
    main()
