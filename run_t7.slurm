#!/bin/bash
#SBATCH --job-name=t7_forecast
#SBATCH --output=t7_%j.out
#SBATCH --error=t7_%j.err
#SBATCH --time=16:00:00
#SBATCH --partition=gpu
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=128G

# 1) Load & activate your Conda env
module load Anaconda3
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate bigdata

# 2) Launch the Dask scheduler on node 0
if [ "$SLURM_NODEID" -eq 0 ]; then
  dask scheduler --port 8786 --dashboard-address :8787 &
  export SCHEDULER="$(hostname):8786"
  echo "Scheduler available at $SCHEDULER"
fi

sleep 15  # let scheduler start

# 3) Start one worker per node, cap at ~110 GB so you keep 18 GB headroom
srun dask worker "$SCHEDULER" \
     --nthreads $SLURM_CPUS_PER_TASK \
     --memory-limit 110GB &

sleep 20  # let workers register

# 4) Run the forecasting script
export SCHEDULER
python t7_forecasting.py

wait
