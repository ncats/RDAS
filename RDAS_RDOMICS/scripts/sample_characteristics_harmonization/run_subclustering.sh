#!/bin/bash

#SBATCH --job-name=subclustering
#SBATCH --output=scripts/sample_characteristics_harmonization/logs/subclustering_%j.out
#SBATCH --error=scripts/sample_characteristics_harmonization/logs/subclustering_%j.out
#SBATCH --partition=extended_gpu
#SBATCH --gres=gpu:4
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=200G
#SBATCH --time=2-00:00:00

# Adjust the SBATCH resource lines above for your cluster if needed.
echo "=== Starting LLM Sub-clustering Process ==="
echo "Date: $(date)"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running on node: $(hostname)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${RDAS_HARM_OUTPUT_DIR:-${SCRIPT_DIR}/sample_characteristics_harmonization_output}"
LOG_DIR="${SCRIPT_DIR}/logs"
CLUSTERED_FILE="${RDAS_CLUSTERED_FILE:-${OUTPUT_DIR}/4_llm_clustered_sample_characteristics.csv}"
SUBCLUSTERED_FILE="${RDAS_SUBCLUSTERED_FILE:-${OUTPUT_DIR}/5_subclustered_sample_characteristics_v1.csv}"
CONDA_ENV_NAME="${RDAS_LLM_CONDA_ENV:-step4-expertise}"

mkdir -p "${LOG_DIR}" "${OUTPUT_DIR}"

source ~/anaconda3/etc/profile.d/conda.sh
conda activate "${CONDA_ENV_NAME}"
# Check GPU availability
echo "GPU Information:"
nvidia-smi

# Set environment variables
export CUDA_VISIBLE_DEVICES=0,1,2,3
export VLLM_WORKER_MULTIPROC_METHOD=spawn

cd "${REPO_ROOT}"

# Check input files exist
echo "Checking input files..."
if [ ! -f "${CLUSTERED_FILE}" ]; then
    echo "Error: clustered file not found: ${CLUSTERED_FILE}"
    exit 1
fi

echo "Conda env: ${CONDA_ENV_NAME}"
echo "Input files found. Starting sub-clustering..."

# Run the sub-clustering script
python "${SCRIPT_DIR}/5_llm_subclustering.py"

# Check if successful
if [ $? -eq 0 ]; then
    echo "✓ Sub-clustering completed successfully!"
    
    # Show output files
    echo ""
    echo "Output file created:"
    ls -la "${SUBCLUSTERED_FILE}" 2>/dev/null || echo "Output file not found"
    
    # Show summary statistics
    if [ -f "${SUBCLUSTERED_FILE}" ]; then
        echo ""
        echo "Summary statistics:"
        echo "Total sub-categories: $(tail -n +2 "${SUBCLUSTERED_FILE}" | wc -l)"
        echo "Main categories processed: $(tail -n +2 "${SUBCLUSTERED_FILE}" | cut -d',' -f1 | sort -u | wc -l)"
    fi
    
else
    echo "✗ Sub-clustering failed with exit code $?"
    exit 1
fi

echo ""
echo "=== Sub-clustering Process Complete ==="
echo "Date: $(date)" 
