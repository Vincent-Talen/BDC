# Assignment 3
**[Using GNU Parallel to distribute processing](https://bioinf.nl/~martijn/BDC/opdracht3.html)**


---
## About this assignment
Manually dividing the work, distributing it over multiple computers and also having to manually start a terminal (and SSH session) for each worker,
is not very practical and efficient, this is why we'll let GNU Parallel do the work for us. Using GNU Parallel we can let it divide the files up in chunks and
distribute them over the network through SSH automatically.

### Grading
Like the other assignments the testing is done by an automated script that checks the output of the pipeline.
Next to the output it will also be checked if parallel is used and if it distributes the work over at least 2 nodes on the network.

The testing of the script will be done on the Hanze Bio-Informatics network inside the `commons/conda/dsls` conda environment.

To execute the bash script the following command is used:
```bash
bash assignment3.sh
```

Another requirement to pass the assignment is that a minimum `pylint` score of 8.0/10.0 is required, this will be checked with the following command:
```bash
pylint --disable C0301,E1101 assignment3.py
```


---
## Assignment files

#### assignment3.py
The main script doing the calculations, created for the assignment.

#### assignment3.sh
The bash script with the parallel command that splits and distributes the file to multiple computers.


---
## Installation
For installation instructions, follow the general installation instructions from the [README located in the repository root](https://github.com/Vincent-Talen/BDC#installation).

### GNU Parallel
Because this assignment relies on GNU Parallel, it needs to be installed on the system. For instructions on how to install GNU Parallel, check the [GNU Parallel website](https://www.gnu.org/software/parallel/).

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

Then, set the `Assignment3` directory as your working directory.
It is assumed you yourself have a FastQ file to use, but one is also available on the Hanze Bio-Informatics network; `/commons/Themas/Thema12/HPC/rnaseq.fastq`.
This is also the one hard-coded into the Bash script, so if you want to use another file, you need to change the path in the script.

The only thing having to be done to run this assignment is run the bash script:
```bash
bash assignment3.sh
```
It will automatically get the correct path to the Python script so GNU Parallel can distribute the work over the network.

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
* [GNU Parallel Documentation](https://www.gnu.org/software/parallel/)  


---
## Contact
For support or any other questions, do not hesitate to contact me at v.k.talen@st.hanze.nl
