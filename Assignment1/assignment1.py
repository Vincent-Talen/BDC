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
__version__ = "0.1"

# IMPORTS
import argparse
import multiprocessing as mp

from pathlib import Path

import numpy as np

# GLOBALS
LAST_BATCH_SIZE: int


# FUNCTIONS
def parse_args():
    """Parses the arguments given to the script.

    Returns:
        args: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Script for Assignment 1 of the Big Data Computing course"
    )
    parser.add_argument(
        "-n",
        action="store",
        dest="n",
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


def phred_lines_generator(file_path: Path):
    """Generator that yields only the PHRED scores from a FastQ file.

    Args:
        file_path (Path):
            The path to the FastQ file.

    Yields:
        The PHRED scores of the FastQ file.
    """
    with open(file_path, "r", encoding="UTF-8") as file:
        for i, line in enumerate(file):
            if i % 4 == 3:
                yield line.strip()


def batch_generator(phred_lines, batch_size=5_000):
    """Generator that yields batches of PHRED scores.

    Args:
        phred_lines (generator):
            Generator that yields PHRED scores.
        batch_size (int, optional):
            The size of the batches. Defaults to 5_000.

    Yields:
        List of PHRED scores.
    """
    batch = []
    for line in phred_lines:
        batch.append(line)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        global LAST_BATCH_SIZE
        LAST_BATCH_SIZE = len(batch)
        yield batch


def get_mean_phred_scores(phred_lines_list):
    """Calculates the mean PHRED scores per base position for a list of PHRED scores.

    Args:
        phred_lines_list (list):
            List of PHRED scores.

    Returns:
        The mean PHRED scores per base position.
    """
    ascii_dict = {chr(i): i - 33 for i in range(33, 127)}
    phred_scores = np.array(
        [[ascii_dict[phred_score] for phred_score in phred_line] for phred_line in
         phred_lines_list]
    )
    return np.mean(phred_scores, axis=0)


def main():
    """Main function of the script."""
    # Parse arguments
    args = parse_args()

    # Define batch size
    batch_size = 5_000

    for file_path in args.fastq_files:
        # Generators
        phred_lines = phred_lines_generator(file_path)
        batches = batch_generator(phred_lines, batch_size)

        # Initialize and create multiprocessing pool
        with mp.Pool(processes=args.n) as pool:
            results = pool.map(get_mean_phred_scores, batches)

        # Convert pool results to numpy array
        batch_means = np.array(results)
        # Calculate weights for batches
        weights = np.full(batch_means.shape[0], batch_size)
        weights[-1] = LAST_BATCH_SIZE
        # Calculate weighted average
        file_pos_means = np.average(batch_means, axis=0, weights=weights)

        if output_path := args.csvfile:
            if len(args.fastq_files) > 1:
                output_path = output_path.parent.joinpath(
                    f"{file_path.stem}_{output_path.name}"
                )
            with open(output_path, "w", encoding="UTF-8") as csvfile:
                for i, pos in enumerate(file_pos_means):
                    csvfile.write(f"{i},{pos}\n")
        else:
            print(file_path)
            for i, pos in enumerate(file_pos_means):
                print(f"{i},{pos}")


if __name__ == "__main__":
    main()
