# Big Data Computing
**Bio-Informatics Year 3, Period 12 (2023-2024)**

Learning about the concepts of Big Data Computing, e.g. parallelization, distributed computing, etc. and how to use them in practice.


---
## Table of Contents
- [About the course assignments](#about-the-course-assignments)
- [Repository file structure](#repository-file-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Useful links](#useful-links)
- [Contact](#contact)


---
## About the course assignments
### Assignment1 - Processing FastQ files using multiprocessing.Pool
To get familiar with the concept of splitting workload we start by using multiprocessing to use multiple cores on a single machine.
The script created calculates the average PHRED score per position of FastQ files.

Tools used: `argparse`, `multiprocessing.Pool`, `pathlib.Path`, `numpy`

### Assignment2 - Processing the FastQ files on multiple computers
This assignment is a continuation of assignment1 since it has the same input and output, 
but computing will now be done on multiple computers using `multiprocessing`s `Process` and `Queue` classes.

New tools used: `multiprocessing.Process`, `multiprocessing.Queue`, `queue.Queue`

### Assignment3 - Splitting and distributing FastQ files with GNU Parallel
To not have to manually start a server and clients on multiple computers, `GNU Parallel` is used to distribute the work over the network.
It does not only distribute the work, but it also actually splits the files into smaller chunks and because of this the Python script
now has to read its data directly from `STDIN` using `fileinput`.

New tools used: `fileinput`, `GNU Parallel`


### Assignment4 - Using SLURM and OpenMPI to distribute and perform work on a cluster
On a cluster you may not directly have access to nodes using SSH, so directly distributing it using `GNU Parallel` may not be possible or allowed. 
Often, the available resources need to be shared with other users, so a job scheduler is used to manage the resources and for this assignment `SLURM` is used. 
When using `SLURM` we do not directly have access to or knowledge about the nodes performing the work, another way to keep track of what work needs to be done is needed. 
This is where `OpenMPI` with `mpi4py` comes in, it starts the multiple processes (possibly on different nodes) and handles all the communication between them.

New tools used: `mpi4py`, `OpenMPI`, `SLURM`


### Assignment5 - Using Apache Spark and pyspark.sql to analyse a GBFF file's contents
An even better and more used option is `Apache Spark`, which is an engine made for large-scale data processing (big data) based on the MapReduce paradigm. 
It can distribute work to nodes on a cluster, utilizes in-memory caching and optimizes query execution for fast analytic queries against data of any size. 
Because we are working in Python we will use `pyspark.sql` which is a Spark module that makes it easier to work with structured data using `DataFrame`s. 
This assignment will use these tools and its accompanying techniques to analyse the contents of a `GenBank Flat File` (GBFF) by answering some questions about it.

New tools used: `Apache Spark`, `pyspark.sql`


---
## Repository file structure
```
BDC
├── Assignment1
│   ├── README.md
│   ├── assignment1.py
│   └── example_output.csv
├── Assignment2
│   ├── README.md
│   ├── assignment2.py
│   └── example_output.csv
├── Assignment3
│   ├── README.md
│   ├── assignment3.py
│   ├── assignment3.sh
│   └── example_output.csv
├── Assignment4
│   ├── README.md
│   ├── assignment4.py
│   ├── assignment4.sh
│   └── example_output.csv
├── Assignment5
│   ├── README.md
│   └── assignment5.py
├── .gitignore
├── LICENSE
├── README.md
└── environment.yml
```


---
## Installation
This project is made on, and is only intended for, use on a Linux system. Guarantees can not be made for other operating systems.
The reason is that some methods/techniques used in the assignments are Linux-specific.

### Bioinformatics network
This repository and its scripts are made for a course that is a part of the bioinformatics study, thus they are made to be executed on the bioinformatics network of the Hanze. 
Because the grading of the scripts is done in a certain way and specific programs are used for *High-Throughput Computing* clusters, that are difficult to install and not even meant for normal computers,
it is difficult to run the scripts on a different network, with some even requiring changes to be made to them.
When using the scripts on the bioinformatics network, no installation is required except for cloning this repository.

### A different network/computer
It might be possible to use some scripts on a different network or computer without a lot of effort, but some scripts need a lot more effort and/or changes to them to work.
Because it is outside the scope/intentions of this project to do this, there won't be detailed instructions on how to do this.

Some things that will need to be done may include installing conda and creating an environment, installing SLURM, installing MPI (for python) and installing Spark.


---
## Usage
When running the script of an assignment, the working directory should be set to that of the assignment and the correct conda environment should be activated.
If you already have conda active you can simply use the following to activate the environment:
```bash
conda activate /commons/conda/dsls
```
When you do not have conda active yet, you can activate it and the environment immediately too by running the following command:
```bash
source /commons/conda/conda_load.sh
```

After the environment is activated the scripts can then be run by calling them with their required parameters.  
Each assignment has different methods and arguments that need to be used to execute them, so for these instructions refer to the README of that specific assignment.


---
## Useful links
* [Course Assignments Website](https://bioinf.nl/~martijn/BDC/)  
* [Python Multiprocessing Module](https://docs.python.org/3.8/library/multiprocessing.html)  
* [GNU Parallel](https://www.gnu.org/software/parallel/)  
* [SLURM Workload Manager](https://slurm.schedmd.com/overview.html)  
* [Apache Spark](https://spark.apache.org/)  
* [Tensorflow](https://www.tensorflow.org/)  


---
## Contact
For support or any other questions, do not hesitate to contact me at v.k.talen@st.hanze.nl
