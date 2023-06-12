#!/bin/bash

########################################
#   BDC Assignment3 by Vincent Talen   #
########################################

#SBATCH --job-name=bdc_a3
#SBATCH --account=vktalen
#              d-hh:mm:ss
#SBATCH --time=0-02:00:00
#SBATCH --nodes=1
#SBATCH --partition=assemblix
#SBATCH --nodelist=assemblix2019
#SBATCH --cpus-per-task=20
#SBATCH --mem-per-cpu=4500MB

source /commons/conda/conda_load.sh

export INPUT_FILES=("/commons/Themas/Thema12/HPC/rnaseq.fastq" "/data/datasets/rnaseq_data/Brazil_Brain/SPM26_R1.fastq")
#export INPUT_FILES=("/commons/Themas/Thema12/HPC/rnaseq.fastq")

for file_name in "${INPUT_FILES[@]}" ; do
  parallel --jobs 20 --pipepart --block -1 --regexp --recstart "@.*(/1| 1:.*)\n[A-Za-z\n\.~]" --recend "\n" echo :::: "$file_name"
done
