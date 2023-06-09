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
__version__ = "2.2"

# IMPORTS
import argparse
import multiprocessing as mp
import sys
from operator import itemgetter

from pathlib import Path
from typing import BinaryIO

import numpy as np


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


def combine_numpy_arrays(array_list: list[np.ndarray], *, phred: bool = False):
    # Create array with the length of every line
    row_lengths = np.array([len(item) for item in array_list])
    # Create 2-D boolean array indicating if lines have a character at a position
    bool_array = row_lengths[:, None] > np.arange(row_lengths.max())
    # Create 2-D array containing zeros in the same shape as the boolean array
    complete_array = np.zeros(bool_array.shape, dtype=int)

    # Fill the data normally or if phred is true perform ASCII-33 conversion
    if phred:
        # Place the lines' phred scores (ascii-33) into the 2-D array
        complete_array[bool_array] = np.concatenate(array_list) - 33
    else:
        # Place the data into the 2-D array
        complete_array[bool_array] = np.concatenate(array_list)
    return complete_array


# CLASSES
class FastQChunk:
    def __init__(self, filepath: Path, start_offset: int, stop_offset: int):
        self.filepath: Path = filepath
        self.start_offset: int = start_offset
        self.stop_offset: int = stop_offset

        self.sum_array: np.ndarray = None
        self.position_count_array: np.ndarray = None

    def perform_stuff(self):
        # Get the quality lines as ascii unsigned integers in numpy arrays
        quality_array_list = [
            np.frombuffer(line, dtype=np.uint8)
            for line in self.quality_line_generator()
        ]

        # Create a single array containing all the quality lines' phred scores
        complete_phred_array = combine_numpy_arrays(quality_array_list, phred=True)

        # Calculate the sum and count/weight of each column for the chunk
        self.sum_array = np.sum(complete_phred_array, axis=0)
        self.position_count_array = np.count_nonzero(complete_phred_array, axis=0)

        # Return the current chunk instance
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
        self,
        fastq_files: list[Path],
        output_file: Path | None,
        chunk_count: int = 4,
        min_chunk_size: int = 1024
    ):
        self.file_paths: list[Path] = fastq_files
        self.output_file: Path | None = output_file
        self.chunk_count: int = chunk_count
        self.min_chunk_size: int = min_chunk_size
        self._check_if_input_files_exist_and_not_empty()

    def _check_if_input_files_exist_and_not_empty(self):
        file_error_strings = []
        for file_path in self.file_paths:
            if not file_path.exists():
                file_error_strings.append(f"ERROR: File '{file_path}' does not exist!")
            if not file_path.stat().st_size > 0:
                file_error_strings.append(f"ERROR: File '{file_path}' is empty!")
        if file_error_strings:
            print("\n".join(file_error_strings))
            print("Exiting...")
            sys.exit(1)

    def _proportionally_divide_chunks_between_files(self) -> dict[Path, int]:
        # Initialize a dictionary with each file having 1 initial chunk
        file_chunks_dict = {file_path: 1 for file_path in self.file_paths}

        # Get the byte size of each file and unallocated chunks after giving each file 1
        file_sizes = [file_path.stat().st_size for file_path in self.file_paths]
        unallocated_chunks = self.chunk_count - len(file_sizes)

        # Calculate the quota at which size an extra core is given to a file
        quota = sum(file_sizes) / (1 + unallocated_chunks)
        # Calculate the fractional amount of times the quota fits in the file byte sizes
        fractions = [file_size / quota for file_size in file_sizes]

        # Give each file the amount of chunks that fully fit in the file size
        for i, file_path in enumerate(file_chunks_dict):
            allocated_chunks = int(fractions[i])
            file_chunks_dict[file_path] += allocated_chunks
            unallocated_chunks -= allocated_chunks
            fractions[i] -= allocated_chunks

        # If not all chunks are allocated, add to the files with the largest remainders
        if unallocated_chunks > 0:
            # Bind the file paths to their remaining fractions and apply descending sort
            fraction_tuples = (
                (file, frac) for file, frac in zip(self.file_paths, fractions)
            )
            sorted_fractions = sorted(fraction_tuples, key=itemgetter(1), reverse=True)
            # Give `unallocated chunks` amount of files an extra core
            for file_path, _ in sorted_fractions[:unallocated_chunks]:
                file_chunks_dict[file_path] += 1

        # Return the dictionary with the file paths and their allocated chunks
        return file_chunks_dict

    def chunk_generator(self):
        if len(self.file_paths) == 1:
            # When only given one file it can use all the chunks
            file_chunks_dict = {self.file_paths[0]: self.chunk_count}
        elif len(self.file_paths) < self.chunk_count:
            # If there are more chunks than files, divide chunks proportionally
            file_chunks_dict = self._proportionally_divide_chunks_between_files()
        else:
            # Use 1 chunk per file if there are more, or as many files as chunks
            file_chunks_dict = {file_path: 1 for file_path in self.file_paths}

        for filepath in self.file_paths:
            # Get the byte size and allocated chunks for the current file
            file_byte_size = filepath.stat().st_size
            allocated_chunks = file_chunks_dict[filepath]

            # Calculate chunk sizes and remainder of bytes
            quotient, remainder = divmod(file_byte_size, allocated_chunks)

            # Enforce minimum chunk size
            if quotient < self.min_chunk_size:
                allocated_chunks = file_byte_size // self.min_chunk_size
                quotient, remainder = divmod(file_byte_size, allocated_chunks)

            # Calculate the start and stop byte offsets and yield FastQChunk objects
            for i in range(allocated_chunks):
                start = i * quotient + min(i, remainder)
                stop = (i + 1) * quotient + min(i + 1, remainder)
                yield FastQChunk(filepath, start, stop)

    def process_results(self, processed_chunks: list[FastQChunk]):
        # Create dictionary to put chunks in per file
        chunks_per_file = {file_path: [] for file_path in self.file_paths}

        # Put chunk result arrays in the dictionary
        for chunk in processed_chunks:
            chunk_results = [chunk.sum_array, chunk.position_count_array]
            chunks_per_file[chunk.filepath].append(chunk_results)

        # Calculate the total average phred score per position per file
        for file, array_lists in chunks_per_file.items():
            file_phred_sum_arrays = [item[0] for item in array_lists]
            file_position_count_arrays = [item[1] for item in array_lists]

            # Combine the sums and position counts of all the chunks of the file
            file_total_phred_sum = np.sum(
                combine_numpy_arrays(file_phred_sum_arrays), axis=0
            )
            file_total_position_counts = np.sum(
                combine_numpy_arrays(file_position_count_arrays), axis=0
            )

            # Calculate the total average phred score per position for the file
            file_phred_averages = np.divide(
                file_total_phred_sum, file_total_position_counts, dtype=np.float64
            )
            # Finalize by saving or displaying the results
            self.show_results_for_file(file, file_phred_averages)

    def show_results_for_file(
        self, input_file_path: Path, file_phred_averages: np.ndarray
    ):
        if output_path := self.output_file:
            if len(self.file_paths) > 1:
                output_path = self.output_file.parent.joinpath(
                    f"{input_file_path.stem}_{self.output_file.name}"
                )
            with open(output_path, "w", encoding="UTF-8") as csvfile:
                for i, pos in enumerate(file_phred_averages):
                    csvfile.write(f"{i},{pos}\n")
        else:
            print(input_file_path)
            for i, pos in enumerate(file_phred_averages):
                print(f"{i},{pos}")


def main():
    """Main function of the script."""
    # Parse arguments
    args = parse_args()

    # Create file handler and use it to generate chunks
    file_handler = FastQFileHandler(
        fastq_files=args.fastq_files,
        output_file=args.output_file,
        chunk_count=args.cpu_count,
        min_chunk_size=1024
    )
    unprocessed_chunks = file_handler.chunk_generator()

    # Initialize and create multiprocessing pool
    with mp.Pool(processes=args.cpu_count) as pool:
        processed_chunks = pool.map(FastQChunk.perform_stuff, unprocessed_chunks)

    # Finalize by further processing the results
    file_handler.process_results(processed_chunks)


if __name__ == "__main__":
    main()
