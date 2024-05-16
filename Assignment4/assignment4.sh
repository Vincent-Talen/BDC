#!/bin/bash

########################################
#   BDC Assignment4 by Vincent Talen   #
########################################

#SBATCH --job-name=bdc_a4
#SBATCH --account=vktalen
#              d-hh:mm:ss
#SBATCH --time=0-02:00:00
#SBATCH --ntasks=5
#SBATCH --ntasks-per-node=1
#SBATCH --partition=workstations
# SBATCH --partition=assemblix
# SBATCH --nodelist=assemblix2019

source /commons/conda/conda_load.sh

#export INPUT_FILES=("/commons/Themas/Thema12/HPC/rnaseq.fastq" "/data/datasets/rnaseq_data/Brazil_Brain/SPM26_R1.fastq")
export INPUT_FILES=("/commons/Themas/Thema12/HPC/rnaseq.fastq")

mpiexec -np 5 python3 assignment4.py "${INPUT_FILES[@]}"
