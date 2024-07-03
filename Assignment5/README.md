# Assignment 5
**[Using Apache Spark and pyspark.sql to analyse a GBFF file's contents](https://bioinf.nl/~martijn/BDC/opdracht5.html)**


---
## About this assignment
An even better and more used option is `Apache Spark`, which is an engine made for large-scale data processing (big data) based on the MapReduce paradigm. 
It can distribute work to nodes on a cluster, utilizes in-memory caching and optimizes query execution for fast analytic queries against data of any size. 
Because we are working in Python we will use `pyspark.sql` which is a Spark module that makes it easier to work with structured data using `DataFrame`s. 
This assignment will use these tools and its accompanying techniques to analyse the contents of a `GenBank Flat File` (GBFF) by answering some questions about it.


### Grading
The grading for this assignment differs from that of the previous ones, because the output of this assignment are a couple of answers to a question without a specific format, 
thus it will be manually checked and graded by the teacher. Like the other assignments it will be executed on the Hanze Bio-Informatics network.

To run the python script the following command is used:
```bash
python3 assignment5.py
```

Another requirement to pass the assignment is that a minimum `pylint` score of 8.0/10.0 is required, this will be checked with the following command:
```bash
pylint --disable C0301,E1101 assignment5.py
```


---
## Assignment files

#### assignment5.py
Script that loads in a GBFF file using pyspark and extracts all the features from it to answer 4 questions about them.


---
## Installation
For installation instructions, follow the general installation instructions from the [README located in the repository root](https://github.com/Vincent-Talen/BDC#installation).

### Spark
Because this assignment uses Spark to distribute and perform the work, it needs to be installed on the system.
For instructions on how to install Spark, check the [Spark Documentation](https://spark.apache.org/docs/latest/).


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

Then, set the `Assignment5` directory as your working directory.
It is assumed that you have access to the `/data/datasets`, since some Archaea files have been hardcoded inside the script that are used.
If you want to use the tool on a different file, you can change the path in the `main` function of the script.

Then, simply execute the script using the following command:
```bash
python3 assignment5.py
```

The output of the script will be printed in the terminal and look like this:
```
Performing analysis and answering questions for the following file:
	/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff

Answer 1: An Archaea genome has 52.33 features on average.
Answer 2: The proportion between coding and non-coding features is 51.03 to 1.
Answer 3: The minimum amount of proteins in any organism is 2 and the maximum is 98721.
Answer 4: The average length of a feature is 846.57.

Saved the DataFrame with only the coding features to:
    archaea.1.genomic_coding_features.parquet
```


---
## Useful links
* [Pathlib Documentation](https://docs.python.org/3.10/library/pathlib.html)  
* [Pyspark Documentation](https://spark.apache.org/docs/latest/api/python/index.html)


---
## Contact
For support or any other questions, do not hesitate to contact me at v.k.talen@st.hanze.nl
