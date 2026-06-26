#!/bin/bash

#SBATCH --job-name=prepare_properties
#SBATCH --output=scripts/sample_characteristics_harmonization/logs/prepare_properties_%j.out
#SBATCH --error=scripts/sample_characteristics_harmonization/logs/prepare_properties_%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:00:00

# Adjust the SBATCH resource lines above for your cluster if needed.
echo "=== Preparing Sample Properties by Subcategory ==="
echo "Date: $(date)"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running on node: $(hostname)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${RDAS_HARM_OUTPUT_DIR:-${SCRIPT_DIR}/sample_characteristics_harmonization_output}"
LOG_DIR="${SCRIPT_DIR}/logs"
SUBCLUSTER_FILE="${RDAS_SUBCLUSTER_FILE:-${OUTPUT_DIR}/5_subclustered_sample_characteristics_v2_expert_consolidated.csv}"
SAMPLE_NODE_FILE="${RDAS_SAMPLE_NODE_FILE:-${REPO_ROOT}/scripts/data/node_csv_files/sample_node.csv}"
SAMPLE_PROPERTIES_FILE="${RDAS_SAMPLE_PROPERTIES_FILE:-${OUTPUT_DIR}/sample_properties_by_subcategory.csv}"
CONDA_ENV_NAME="${RDAS_PREP_CONDA_ENV:-step4-expertise}"

mkdir -p "${LOG_DIR}" "${OUTPUT_DIR}"

# Activate environment
source ~/anaconda3/etc/profile.d/conda.sh
conda activate "${CONDA_ENV_NAME}"

cd "${REPO_ROOT}"

# Check inputs
echo "Checking input files..."
if [ ! -f "${SUBCLUSTER_FILE}" ]; then
  echo "Error: subcluster file not found: ${SUBCLUSTER_FILE}"
  exit 1
fi
if [ ! -f "${SAMPLE_NODE_FILE}" ]; then
  echo "Error: sample_node.csv not found: ${SAMPLE_NODE_FILE}"
  exit 1
fi

echo "Conda env: ${CONDA_ENV_NAME}"
echo "Inputs found. Running preparation script..."
python "${SCRIPT_DIR}/6_prepare_sample_properties.py"

if [ $? -eq 0 ]; then
  echo "\n✓ Sample properties preparation completed successfully!"
  if [ -f "${SAMPLE_PROPERTIES_FILE}" ]; then
    echo "Output file: ${SAMPLE_PROPERTIES_FILE}"
    echo "Rows: $(tail -n +2 "${SAMPLE_PROPERTIES_FILE}" | wc -l)"
    echo "Columns: $(head -n 1 "${SAMPLE_PROPERTIES_FILE}" | awk -F"," '{print NF}')"
    echo "\nPreview of columns:"
    head -n 1 "${SAMPLE_PROPERTIES_FILE}" | tr ',' '\n' | nl | sed -n '1,30p'
  else
    echo "Warning: Output file not found after run."
  fi
else
  echo "✗ Preparation failed with exit code $?"
  exit 1
fi

echo "\n=== Preparation Complete ==="
echo "Date: $(date)"
