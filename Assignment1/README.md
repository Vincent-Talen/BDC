# Assignment 1
**[Processing FastQ files using multiprocessing.Pool](https://bioinf.nl/~martijn/BDC/opdracht1.html)**


---
## About this assignment
To get familiar with the Python multiprocessing library and other tools, a practical assignment is given.

The assignment is to create a script that takes one or more FastQ files as input and calculates, 
for each file separately, the average PHRED score for each position of the reads in the file.
This should be done using the multiprocessing.Pool module and three of the main things learned will be 
how to split the files up into chunks, how to process these chunks in parallel and finally how to combine the results.

### Grading
Like the other assignments the testing is done by an automated script that checks the functionality of the pipeline, 
and it will also be checked if the multiprocessing is correctly implemented.

The testing of the script will be done on the Hanze Bio-Informatics network inside the `commons/conda/dsls` conda environment.
The tool will call the script with the following command:
> python3 assignment1.py -n 4 fastq_file1.fastq

Another requirement to pass the assignment is that a minimum `pylint` score of 8.0/10.0 is required, this will be checked with the following command:
> pylint --disable C0301 assignment1.py


---
## Assignment files

#### assignment1.py
The main script created for the assignment.

#### example_output.csv
A .csv output file supplied by the course specifying exactly what the output should look like.


---
## Installation
For installation instructions, follow the general installation instructions from the [README located in the repository root](https://github.com/Vincent-Talen/BDC#installation).


---
## Usage
The first thing to do is activate the Conda environment, the one primarily used for this course is the one that is available on the Hanze Bio-Informatics network.
This environment is located in the `/commons/conda` directory under the name `dsls`.

If you already have a conda or mamba installation just use the following command:
> conda activate /commons/conda/dsls

If you do not have a conda or mamba installation, you can use the following command to activate the environment through a shell script:
> source /commons/conda/conda_load.sh

Then, set the `Assignment1` directory as your working directory.
It is assumed you yourself have a FastQ file to use, but one is also available on the Hanze Bio-Informatics network; `/commons/Themas/Thema12/HPC/rnaseq.fastq`.

An example of how to run the script is shown below:
> python3 assignment1.py -n 4 /commons/Themas/Thema12/HPC/rnaseq.fastq

To see all available options, use the `-h` option:
> python3 assignment1.py -h

### Output
By default, all output is printed to the command line, but it is also possible to save the output to a file by using the `-o` option as can be seen below:
> python3 assignment1.py -n 4 -o output.csv /commons/Themas/Thema12/HPC/rnaseq.fastq

If multiple input files are specified their output to the terminal is split by the file names. 
When the `-o` option is used with multiple input files, the output file is used as a suffix in combination with the input file names.
For example, the input files `rnaseq.fastq` and `my_own_file.fastq` will have output files with the names `rnaseq_output.csv` and `my_own_file_output.csv`.

The format of the scripts output is shown below, the full output file named [example_output.csv](example_output.csv) can also be checked.
```csv
0,32.63915044997766 
1,32.91791978116926
2,33.038218404050866
```


---
## Useful links
* [Argparse Documentation](https://docs.python.org/3.10/library/argparse.html)  
* [Multiprocessing Documentation](https://docs.python.org/3.10/library/multiprocessing.html)  
* [Pathlib Documentation](https://docs.python.org/3.10/library/pathlib.html)  
* [NumPy Documentation](https://numpy.org/doc/stable/)  


---
## Contact
For support or any other questions, do not hesitate to contact me at v.k.talen@st.hanze.nl
