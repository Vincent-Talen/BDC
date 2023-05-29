# Big Data Computing
**Bio-Informatics Year 3, Period 12 (2022-2023)**

Description of the course.


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
???


---
## Repository file structure
```
BDC
├── Assignment1
│   ├── README.md
│   ├── assignment1.py
│   └── example_output.csv
├── .gitignore
├── LICENSE
└── README.md
```


---
## Installation
This project is made on, and only intended for, use on a Linux system. Guarantees can not be made for other operating systems.

### Conda/Mambaforge
To manage the virtual environment and dependencies the [Conda](https://conda.io/)-based Python3 distribution [Mambaforge](https://github.com/conda-forge/miniforge#mambaforge) is used.
Mambaforge provides the required Python and Conda commands, but also includes Mamba, an extremely fast and robust replacement for the Conda package manager. 
Since the default conda solver is large, slow and sometimes has issues with selecting the latest package releases.

Download the latest installer script of Mambaforge for your OS from https://github.com/conda-forge/miniforge#mambaforge and follow the instructions listed there to install it.

### Create environment
The first thing that needs to be done before being able to run the scripts, is to create the environment with all the required dependencies. 
This is easily done using Mamba and the `environment.yml` file included in this repository.

Open a terminal and ensure your terminal has the base mamba environment activated with
```bash
mamba activate base
```
Then make sure the working directory is the root of this repository and then simply use the following command to create the environment:
```bash
mamba env create
```
This will name the environment `bdc`, if desired it can be given another name by adding `--name your-desired-name`.


---
## Usage
When running the script of an assignment, the working directory should be set to that of the assignment and the `bdc` environment should be activated.
Activate the environment by running the following command (if your environment is named differently replace `bdc` with that name):
```bash
mamba activate bdc
```
After the environment is activated the scripts can then be run by calling them with their required parameters.  
Each assignment can have different arguments and input data that needs to be gathered, so for more instructions the README for that assignment should be checked.


---
## Useful links
* [Course Assignments Website](https://bioinf.nl/~martijn/BDC/BDC.html)  
* [Python Multiprocessing Module](https://docs.python.org/3.8/library/multiprocessing.html)  
* [GNU Parallel](https://www.gnu.org/software/parallel/)  
* [SLURM Workload Manager](https://slurm.schedmd.com/overview.html)  
* [Apache Spark](https://spark.apache.org/)  
* [Tensorflow](https://www.tensorflow.org/)  


---
## Contact
For support or any other questions, do not hesitate to contact me at v.k.talen@st.hanze.nl
