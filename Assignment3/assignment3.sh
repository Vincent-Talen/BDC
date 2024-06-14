#!/bin/bash

########################################
#   BDC Assignment3 by Vincent Talen   #
########################################

# Define which hosts the work should be distributed to
HOSTS="assemblix2019,nuc404,nuc420"
#HOSTS="assemblix2019,nuc{112..122},nuc{200..208},nuc{401..407},nuc{409..415},nuc{417..427}"

# Define the input file
INPUT_FILE="/commons/Themas/Thema12/HPC/rnaseq.fastq"
#INPUT_FILE="/data/datasets/rnaseq_data/Brazil_Brain/SPM26_R1.fastq"

readonly ASSIGNMENT3_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
readonly PYTHON_SCRIPT="${ASSIGNMENT3_DIR}/assignment3.py"
readonly HEADER_REGEX="@.*(/1| 1:.*)\n[A-Za-z\n\.~]"

parallel -S "${HOSTS}" --jobs 6 --pipepart --block -1 --regexp --recstart "${HEADER_REGEX}" --recend "\n" python3 "${PYTHON_SCRIPT}" --chunk :::: "${INPUT_FILE}" \
  | python3 "${PYTHON_SCRIPT}" --combine -o "output.csv"
