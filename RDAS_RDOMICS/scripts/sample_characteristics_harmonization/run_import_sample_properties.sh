#!/bin/bash

#SBATCH --job-name=import_sample_props
#SBATCH --output=scripts/sample_characteristics_harmonization/logs/import_sample_props_%j.out
#SBATCH --error=scripts/sample_characteristics_harmonization/logs/import_sample_props_%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=2-00:00:00

# Adjust the SBATCH resource lines above for your cluster if needed.
echo "=== Importing Sample Properties into Memgraph ==="
echo "Date: $(date)"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running on node: $(hostname)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${RDAS_HARM_OUTPUT_DIR:-${SCRIPT_DIR}/sample_characteristics_harmonization_output}"
LOG_DIR="${SCRIPT_DIR}/logs"
INPUT_CSV="${RDAS_SAMPLE_PROPERTIES_FILE:-${OUTPUT_DIR}/sample_properties_by_subcategory.csv}"
RESUME_FILE="${RDAS_RESUME_FILE:-${OUTPUT_DIR}/sample_import_resume.json}"
CONDA_ENV_NAME="${RDAS_IMPORT_CONDA_ENV:-socialnetwork310}"

mkdir -p "${LOG_DIR}" "${OUTPUT_DIR}"

echo "Logs will stream to: ${LOG_DIR}/import_sample_props_${SLURM_JOB_ID}.out"
echo "You can follow progress with: tail -f ${LOG_DIR}/import_sample_props_${SLURM_JOB_ID}.out &"

# Activate environment
source ~/anaconda3/etc/profile.d/conda.sh
conda activate "${CONDA_ENV_NAME}"

cd "${REPO_ROOT}"

# Check input
if [ ! -f "${INPUT_CSV}" ]; then
  echo "Error: input CSV not found: ${INPUT_CSV}"
  exit 1
fi

echo "Starting import script..."
echo "Conda env: ${CONDA_ENV_NAME}"
echo "Expected env vars: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD"
PYTHONUNBUFFERED=1 python -u "${SCRIPT_DIR}/7_import_to_sample_node.py" --resume_file "$RESUME_FILE"

if [ $? -eq 0 ]; then
  echo "\n✓ Import completed successfully!"
else
  echo "✗ Import failed with exit code $?"
  echo "If interrupted, you can resume automatically using the resume file: $RESUME_FILE"
  exit 1
fi

echo "\n=== Import Complete ==="
echo "Date: $(date)" 
