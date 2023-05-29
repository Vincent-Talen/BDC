# Assignment 2
**[Processing the FastQ files on multiple computers](https://bioinf.nl/~martijn/BDC/opdracht2.html)**


---
## About this assignment
To learn about how computers and networks used, this assignment is a continuation of Assignment1 
but the computing will now be done on multiple computers using `multiprocessing`s `Process` and `Queue` classes.

### Grading
Like the other assignments the testing is done by an automated script that checks the functionality of the pipeline, 
and it will also be checked if the multiprocessing is correctly implemented.

The testing of the script will be done on the Hanze Bio-Informatics network inside the `commons/conda/dsls` conda environment.
The tool will start both a server, and one or more clients that connect to the server through a socket.

To start the server the following command is used:
> python3 assignment2.py -s --host localhost --port 25717 --chunks 100 /commons/Themas/Thema12/HPC/rnaseq.fastq

Clients are then started using this command:
> python3 assignment2.py -c --host localhost --port 25717 -n 4

Another requirement to pass the assignment is that a minimum `pylint` score of 8.0/10.0 is required, this will be checked with the following command:
> pylint --disable C0301 assignment2.py


---
## Assignment files

#### assignment2.py
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

Then, set the `Assignment2` directory as your working directory.
It is assumed you yourself have a FastQ file to use, but one is also available on the Hanze Bio-Informatics network; `/commons/Themas/Thema12/HPC/rnaseq.fastq`.

Both a server and clients need to be started to run the script, the server is started with the `-s` option and the clients with the `-c` option.
They need to be started in separate terminals, either actually on different physical computers or just through SSH. 

Make sure to start the server before the clients and that the host and port are the same for both, 
since this determine the socket the server and clients connect through. 

Example command for starting the server:
> python3 assignment2.py -s --host localhost --port 25717 --chunks 100 /commons/Themas/Thema12/HPC/rnaseq.fastq

Example command for starting the clients:
> python3 assignment2.py -c --host localhost --port 25717 -n 4

To see all available options, use the `-h` option:
> python3 assignment2.py -h

### Output
By default, all output is printed to the command line of the server, but it is also possible to save the output to a file by using the `-o` option as can be seen below:
> python3 assignment2.py -s --host localhost --port 25717 --chunks 100 -o output.csv /commons/Themas/Thema12/HPC/rnaseq.fastq

If multiple input files are specified their output to the terminal is split by the file names. 
When the `-o` option is used with multiple input files, the output file is used as a suffix in combination with the input file names.
For example, the input files `rnaseq.fastq` and `another_file.fastq` will have output files with the names `rnaseq_output.csv` and `another_file_output.csv`.

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
