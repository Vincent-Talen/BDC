#!/bin/bash

#####################################################
#   BDC Assignment4 Benchmarking by Vincent Talen   #
#####################################################

#SBATCH --job-name=bdc_a4_benchmarking
#              d-hh:mm:ss
#SBATCH --time=0-09:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=5
#SBATCH --cpus-per-task=1
#SBATCH --mem=50G
#SBATCH --partition=assemblix
#SBATCH --nodelist=assemblix2019

#  Create the output directory
mkdir -p ./output/

# Initialise the timings CSV file by writing the header
export timings_file="./output/timings.csv"
echo "Ranks,Run,Time" > "${timings_file}"

# Activate the master conda environment
# source /commons/conda/conda_load.sh;

# Define input files
export INPUT_FILES=("/commons/Themas/Thema12/HPC/rnaseq.fastq")

for rank_amount in {2..5}; do
    for run_num in {1..3}; do
        echo "Ranks=${rank_amount} - Run${run_num}: Started at $(date +"%d-%m-%y %T")"
        # Run the Python script with mpiexec, using -np to specify the number of ranks
        start_time=$(date +%s.%N)
        output_file="./output/${rank_amount}ranks_run${run_num}.csv"
        mpiexec -np "${rank_amount}" python3 assignment4.py "${INPUT_FILES[@]}" -o "${output_file}"
        end_time=$(date +%s.%N)

        # Calculate the elapsed time in seconds and save it to the timings file
        elapsed_time=$(echo "${end_time} - ${start_time}" | bc)
        echo "${rank_amount},${run_num},${elapsed_time}" >> "${timings_file}"
    done
done
