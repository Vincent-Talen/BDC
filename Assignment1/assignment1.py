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
from typing import BinaryIO

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
class FastQChunk:
    def __init__(self, filepath: Path, start_offset: int, stop_offset: int):
        self.filepath: Path = filepath
        self.start_offset: int = start_offset
        self.stop_offset: int = stop_offset

        self.mean_array: np.ndarray = None
        self.line_count: int = None

    def perform_stuff(self):
        # Get the quality lines as ascii unsigned integers in numpy arrays
        quality_array_list = [
            np.frombuffer(line, dtype=np.uint8)
            for line in self.quality_line_generator()
        ]

        # Create 2-D array with the lines' phred scores (ascii-33)
        complete_array = np.array(quality_array_list) - 33

        # Calculate the mean per chunk
        self.mean_array = np.mean(complete_array, axis=0, dtype=np.float64)
        # Get the amount of lines in the chunk
        self.line_count = complete_array.shape[0]

        # Return this chunk object back
        return self

    def quality_line_generator(self):
        with open(self.filepath, "rb") as file:
            # Go to the chunk start offset in the file
            file.seek(self.start_offset)

            # Make sure the cursor is positioned at the start of an entry
            self._ensure_correct_positioning(file)

            # Keep reading entries until the chunk stop offset is reached
            while file.tell() < self.stop_offset:
                # Skip the header, sequence and separator lines
                file.readline()
                file.readline()
                file.readline()
                # Yield the quality line
                yield file.readline().strip()

    def _ensure_correct_positioning(self, file_obj: BinaryIO):
        correctly_positioned: bool = False
        while not correctly_positioned and file_obj.tell() < self.stop_offset:
            # Remember the byte position before the line is read
            byte_position = file_obj.tell()

            # Read the next line, it should be a header line if it starts with an @
            first_line = file_obj.readline()
            if first_line.startswith(b"@"):
                # By chance the quality line can actually also start with an @
                # To be sure which line we're at we'll check the next line after it too
                byte_position_before_second_line = file_obj.tell()
                second_line = file_obj.readline()
                if second_line.startswith(b"@"):
                    # If the second line starts with @, the first was the quality line
                    # So the second is the header, which we want to position in front of
                    byte_position = byte_position_before_second_line
                    correctly_positioned = True
                else:
                    # If the second line does not start with @, the first was the header
                    # Do nothing since byte_position is already the correct position
                    correctly_positioned = True

        # Go back to the correct byte position in the file object
        file_obj.seek(byte_position)


class FastQFileHandler:
    def __init__(
        self, fastq_files: list[Path], chunk_count: int, min_chunk_size: int = 1024
    ):
        self.file_paths: list[Path] = fastq_files
        self.chunk_count: int = chunk_count
        self.min_chunk_size: int = min_chunk_size
        self._check_if_input_files_exist()

    def _check_if_input_files_exist(self):
        file_error_strings = []
        for file_path in self.file_paths:
            if not file_path.exists():
                file_error_strings.append(f"ERROR: File '{file_path}' does not exist!")
        if file_error_strings:
            print("\n".join(file_error_strings))
            print("Exiting...")
            sys.exit(1)

    def chunk_generator(self):
        for filepath in self.file_paths:
            # Get the byte size of the current file
            file_byte_size = filepath.stat().st_size

            # Calculate chunk sizes and remainder of bytes
            quotient, remainder = divmod(file_byte_size, self.chunk_count)

            # Enforce minimum chunk size
            if quotient < self.min_chunk_size:
                new_chunk_count = file_byte_size // self.min_chunk_size
                quotient, remainder = divmod(file_byte_size, new_chunk_count)

            # Calculate the start and stop byte offsets and yield FastQChunk objects
            for i in range(self.chunk_count):
                start = i * quotient + min(i, remainder)
                stop = (i + 1) * quotient + min(i + 1, remainder)
                yield FastQChunk(filepath, start, stop)


def main():
    """Main function of the script."""
    # Parse arguments
    args = parse_args()

    # Create file handler and use it to generate chunks
    file_handler = FastQFileHandler(
        fastq_files=args.fastq_files, chunk_count=args.cpu_count, min_chunk_size=1024
    )
    chunks: list[FastQChunk] = list(file_handler.chunk_generator())

    # Initialize and create multiprocessing pool
    with mp.Pool(processes=args.cpu_count) as pool:
        chunk_obj_list = pool.map(FastQChunk.perform_stuff, chunks)

    for chunk_obj in chunk_obj_list:
        print(chunk_obj.mean_array)

    for result in results:
        print(len(result))


if __name__ == "__main__":
    main()
