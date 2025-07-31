#!/bin/bash
#SBATCH --job-name=T4_EDA
#SBATCH --output=t4_%j.out
#SBATCH --error=t4_%j.err
#SBATCH --time=04:00:00
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=60G

# 1) Load Anaconda and initialize conda
module load Anaconda3
eval "$(conda shell.bash hook)"
conda activate phil_env

# 2) Cd into the folder containing t4_eda.py
cd /d/hpc/projects/FRI/phil/T4

echo "[$(date)] Starting Task 4 EDA"
python t4_eda.py
echo "[$(date)] Finished Task 4 EDA"
