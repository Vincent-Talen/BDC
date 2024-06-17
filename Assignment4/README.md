# Assignment 4
**[Using SLURM and OpenMPI to distribute and perform work on a cluster](https://bioinf.nl/~martijn/BDC/opdracht4.html)**


---
## About this assignment
On a cluster you may not directly have access to nodes using SSH, so directly distributing it using GNU Parallel may not be possible or allowed. 
Often, the available resources need to be shared with other users, so a job scheduler is used to manage the resources and for this assignment SLURM is used. 
When using SLURM we do not directly have access to or knowledge about the nodes performing the work, another way to keep track of what work needs to be done is needed. 
This is where OpenMPI with mpi4py comes in, it starts the multiple processes (possibly on different nodes) and handles all the communication between them.

### Grading
Like the other assignments the testing is done by an automated script that checks the output of the pipeline. 
The testing of the script will be done on the Hanze Bio-Informatics network using SLURM.

To execute the SLURM bash script the following command is used:
```bash
sbatch assignment4.sh
```

Another requirement to pass the assignment is that a minimum `pylint` score of 8.0/10.0 is required, this will be checked with the following command:
```bash
pylint --disable C0301,E1101 assignment4.py
```


---
## Assignment files

#### assignment4.py
The main script doing the calculations, created for the assignment.

#### assignment4.sh
The SLURM bash script containing the configuration settings and the MPI command that runs the Python script.


---
## Installation
For installation instructions, follow the general installation instructions from the [README located in the repository root](https://github.com/Vincent-Talen/BDC#installation).

### SLURM
Because this assignment needs to be started/scheduled using SLURM, SLURM needs to be installed on the cluster/system that is being used. 
For instructions on how to install SLURM, check the [SLURM Documentation](https://slurm.schedmd.com/documentation.html).

### OpenMPI
OpenMPI also needs to be installed and configured on the system, for instructions on how to do this, check the [OpenMPI Documentation](https://www.open-mpi.org/doc/).


---
## Usage
The first thing to do is activate the Conda environment, the one primarily used for this course is the one that is available on the Hanze Bio-Informatics network.
This environment is located in the `/commons/conda` directory under the name `dsls`.

If you already have a conda or mamba installation just use the following command:
```bash
conda activate /commons/conda/dsls
```

If you do not have a conda or mamba installation, you can use the following command to activate the environment through a shell script:
```bash
source /commons/conda/conda_load.sh
```

Then, set the `Assignment4` directory as your working directory.
It is assumed you yourself have a FastQ file to use, but one is also available on the Hanze Bio-Informatics network; `/commons/Themas/Thema12/HPC/rnaseq.fastq`.
This is also the one hard-coded into the Bash script, so if you want to use another file, you need to change the path in the script.

The only thing having to be done to run this assignment is to send the bash script to SLURM:
```bash
sbatch assignment4.sh
```

The format of the scripts output is shown below, the full output file named [example_output.csv](example_output.csv) can also be checked.
```csv
0,32.63915044997766
1,32.91791978116926
2,33.038218404050866
```


---
## Useful links
* [Argparse Documentation](https://docs.python.org/3.10/library/argparse.html)  
* [Pathlib Documentation](https://docs.python.org/3.10/library/pathlib.html)  
* [NumPy Documentation](https://numpy.org/doc/stable/)  
* [mpi4py Documentation](https://mpi4py.readthedocs.io/en/stable/intro.html)  
* [SLURM Documentation](https://slurm.schedmd.com/documentation.html)  


---
## Contact
For support or any other questions, do not hesitate to contact me at v.k.talen@st.hanze.nl
