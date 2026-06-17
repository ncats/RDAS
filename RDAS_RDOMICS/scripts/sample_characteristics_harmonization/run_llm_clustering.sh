#!/bin/bash
#SBATCH --job-name=llm_clustering
#SBATCH --partition=extended_gpu
#SBATCH --nodes=1
#SBATCH --ntasks=32
#SBATCH --gres=gpu:4
#SBATCH --mem-per-cpu=8G
#SBATCH --time=5-00:00:00
#SBATCH -o scripts/sample_characteristics_harmonization/logs/llm_clustering_%j.out
#SBATCH -e scripts/sample_characteristics_harmonization/logs/llm_clustering_%j.out

# Adjust the SBATCH resource lines above for your cluster if needed.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${RDAS_HARM_OUTPUT_DIR:-${SCRIPT_DIR}/sample_characteristics_harmonization_output}"
LOG_DIR="${SCRIPT_DIR}/logs"
CONDA_ENV_NAME="${RDAS_LLM_CONDA_ENV:-step4-expertise}"

mkdir -p "${LOG_DIR}" "${OUTPUT_DIR}"
cd "${REPO_ROOT}"

# Activate environment
source ~/anaconda3/etc/profile.d/conda.sh
conda activate "${CONDA_ENV_NAME}"

# Set CUDA environment variables for vLLM
export CUDA_VISIBLE_DEVICES=0,1,2,3
export VLLM_USE_MODELSCOPE=False

# Print job information
echo "=== LLM-based Sample Characteristics Clustering ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Start time: $(date)"
echo "Working directory: ${REPO_ROOT}"

# Check Python and vLLM installation
echo "Python version: $(python --version)"
echo "vLLM check:"
python -c "import vllm; print(f'vLLM version: {vllm.__version__}')" 2>/dev/null || echo "vLLM not available"
echo "Conda env: ${CONDA_ENV_NAME}"

# Check input files
echo ""
echo "Checking input files:"
LABELS_FILE="${RDAS_LABELS_FILE:-${OUTPUT_DIR}/3_sample_characteristics_key_count_english_only.csv}"
VOCAB_FILE="${RDAS_VOCAB_FILE:-${OUTPUT_DIR}/4_sample_characteristics_vocabulary_fixed.csv}"
CLUSTERED_FILE="${RDAS_CLUSTERED_FILE:-${OUTPUT_DIR}/4_llm_clustered_sample_characteristics.csv}"
CONFIDENCE_FILE="${RDAS_CONFIDENCE_FILE:-${OUTPUT_DIR}/clustering_confidence_report.csv}"

if [ -f "${LABELS_FILE}" ]; then
    echo "✓ Labels file found: ${LABELS_FILE}"
    echo "  Total labels: $(($(wc -l < "${LABELS_FILE}") - 1))"
    echo "  File size: $(du -h "${LABELS_FILE}" | cut -f1)"
else
    echo "✗ Labels file missing: ${LABELS_FILE}"
    exit 1
fi

if [ -f "${VOCAB_FILE}" ]; then
    echo "✓ Vocabulary file found: ${VOCAB_FILE}"
    echo "  Categories: $(($(wc -l < "${VOCAB_FILE}") - 1))"
else
    echo "✗ Vocabulary file missing: ${VOCAB_FILE}"
    exit 1
fi

# Show top labels by count
echo ""
echo "Top 10 labels by occurrence count:"
tail -n +2 "${LABELS_FILE}" | sort -t',' -k2 -nr | head -10 | nl

# Show available categories
echo ""
echo "Available categories for clustering:"
tail -n +2 "${VOCAB_FILE}" | cut -d',' -f1 | nl

echo ""
echo "Starting clustering process..."
echo "Model path: ${RDAS_LLM_MODEL_PATH:-configured in scripts/config/paths.yaml}"
echo "Features: Multi-stage processing (Keyword → Similarity → LLM)"
echo "Expected outputs: ${CLUSTERED_FILE}, ${CONFIDENCE_FILE}"
echo "============================================"

# Run the clustering script
python "${SCRIPT_DIR}/4_llm_clustering_enhanced.py"

# Check exit status
if [ $? -eq 0 ]; then
    echo "============================================"
    echo "✓ Clustering completed successfully!"
    echo "End time: $(date)"
    
    # Display output file info
    if [ -f "${CLUSTERED_FILE}" ]; then
        echo ""
        echo "✓ Main output file created: ${CLUSTERED_FILE}"
        echo "  Lines: $(wc -l < "${CLUSTERED_FILE}")"
        echo ""
        echo "Clustering results by category:"
        echo "Category | Label Count | Total Occurrences"
        echo "---------|-------------|------------------"
        tail -n +2 "${CLUSTERED_FILE}" | while IFS=',' read -r category label_count total_count labels; do
            printf "%-20s | %8s | %15s\n" "$category" "$label_count" "$total_count"
        done
    else
        echo "⚠ Warning: Main output file not found"
    fi
    
    if [ -f "${CONFIDENCE_FILE}" ]; then
        echo ""
        echo "✓ Confidence report created: ${CONFIDENCE_FILE}"
        echo "  Total entries: $(($(wc -l < "${CONFIDENCE_FILE}") - 1))"
        echo ""
        echo "Confidence distribution:"
        tail -n +2 "${CONFIDENCE_FILE}" | cut -d',' -f5 | sort | uniq -c | sort -nr | while read count conf; do
            echo "  $conf: $count labels"
        done
        echo ""
        echo "Method distribution:"
        tail -n +2 "${CONFIDENCE_FILE}" | cut -d',' -f4 | sort | uniq -c | sort -nr | while read count method; do
            echo "  $method: $count labels"
        done
        
        # Show top clustered labels by occurrence
        echo ""
        echo "Top 10 clustered labels by occurrence count:"
        tail -n +2 "${CONFIDENCE_FILE}" | sort -t',' -k3 -nr | head -10 | cut -d',' -f2,3,4,5 | nl
    else
        echo "⚠ Warning: Confidence report not found"
    fi
    
    echo ""
    echo "=== CLUSTERING SUMMARY ==="
    echo "• Multi-stage processing reduces LLM workload"
    echo "• Confidence scoring for quality assessment"
    echo "• Preserves original occurrence counts"
    echo "• Comprehensive reporting for analysis"
    echo ""
    echo "Files created:"
    echo "  - ${CLUSTERED_FILE} (main results)"
    echo "  - ${CONFIDENCE_FILE} (detailed analysis)"
    
else
    echo "✗ Clustering failed with exit code $?"
    echo "End time: $(date)"
    echo ""
    echo "Check the log file for detailed error information:"
    echo "  ${LOG_DIR}/llm_clustering_${SLURM_JOB_ID}.out"
    exit 1
fi 
