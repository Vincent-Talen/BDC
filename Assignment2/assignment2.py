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
def main():
    """Main function of the script."""
    ...


if __name__ == "__main__":
    main()
