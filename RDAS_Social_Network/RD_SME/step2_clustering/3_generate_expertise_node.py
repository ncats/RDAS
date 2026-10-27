"""
Step 4 (current): Generate Expertise Information for Clusters and Individual Authors

This script processes clustering results from step 2 (clustered_GARD_*.csv) and
generates expertise summaries using an LLM. It outputs three CSVs suitable for
loading to a graph database:

1) expertise_node.csv      columns: entity_type, entity_id, gard_id, cluster_label, expertise, response
2) cluster_node.csv        columns: cluster_id, gard_id, cluster_label, num_authors
3) expertise_relationships.csv columns: entity_id, cluster_id, gard_id, cluster_label, author_id, relationship_type

Defaults point to the outputs of 2_generate_clustering.py and the author_information.json
generated in step 0.
"""

import os
import argparse
import pandas as pd
import json
import glob
from typing import Dict, List, Any, Tuple
from tqdm import tqdm

try:
    from vllm import LLM, SamplingParams
except Exception:
    LLM = None
    SamplingParams = None


def initialize_llm(model_path: str, tensor_parallel_size: int, gpu_memory_utilization: float, max_model_len: int):
    if LLM is None:
        raise RuntimeError("vLLM is not installed or not available in this environment.")
    print("Initializing LLM...")
    llm = LLM(
        model=model_path,
        tensor_parallel_size=tensor_parallel_size,
        gpu_memory_utilization=gpu_memory_utilization,
        max_model_len=max_model_len,
    )
    sampling_params = SamplingParams(
        temperature=0.7,
        max_tokens=1536,
        top_p=0.95,
        stop=["</s>"]
    )
    return llm, sampling_params


def load_step1_author_data(step1_file: str) -> Dict[Any, List[Dict[str, Any]]]:
    print(f"Loading author content from {step1_file}")
    with open(step1_file, 'r', encoding='utf-8') as f:
        author_data = json.load(f)
    author_abstracts: Dict[Any, List[Dict[str, Any]]] = {}
    for author in author_data:
        author_id = author.get('author_id')
        full_abstract = author.get('full_abstract', [])
        if author_id is not None and isinstance(full_abstract, list) and full_abstract:
            author_abstracts[author_id] = full_abstract
    print(f"Loaded abstracts for {len(author_abstracts)} authors")
    return author_abstracts


def extract_abstract_content(full_abstract_list: List[Dict[str, Any]]) -> List[str]:
    extracted_content: List[str] = []
    for item in full_abstract_list:
        if isinstance(item, dict):
            title = str(item.get('title', '')).strip()
            abstract = str(item.get('abstract', '')).strip()
            content_type = item.get('type', 'Unknown')
            if title and abstract:
                content = f"[{content_type}] {title}: {abstract}"
            elif title:
                content = f"[{content_type}] {title}"
            elif abstract:
                content = f"[{content_type}] {abstract}"
            else:
                continue
            extracted_content.append(content)
    return extracted_content


def split_abstracts_into_batches(abstracts: List[str], max_batch_size: int = 20000) -> List[List[str]]:
    batches: List[List[str]] = []
    current_batch: List[str] = []
    current_size = 0
    for abstract in abstracts:
        size = len(abstract)
        if current_batch and current_size + size > max_batch_size:
            batches.append(current_batch)
            current_batch = [abstract]
            current_size = size
        else:
            current_batch.append(abstract)
            current_size += size
    if current_batch:
        batches.append(current_batch)
    return batches


def _extract_json_from_text(text: str) -> Dict[str, Any]:
    # Attempt to pull the first valid JSON object from text
    if not text:
        return {}
    start = text.find('{')
    if start == -1:
        return {}
    brace = 0
    end = -1
    for i in range(start, len(text)):
        if text[i] == '{':
            brace += 1
        elif text[i] == '}':
            brace -= 1
            if brace == 0:
                end = i + 1
                break
    if end == -1:
        return {}
    try:
        return json.loads(text[start:end])
    except Exception:
        return {}


def _is_valid_expertise_dict(parsed: Dict[str, Any]) -> bool:
    if not isinstance(parsed, dict):
        return False
    for k in ("expertise_1", "expertise_2", "expertise_3"):
        v = parsed.get(k)
        if not isinstance(v, str) or not v.strip():
            return False
    return True


def generate_iterative_expertise_summary(entity_ids: List[Any], abstract_batches: List[List[str]], gard_id: str, entity_type: str, entity_id: str, llm, sampling_params) -> Dict[str, Any]:
    print(f"Generating expertise for {entity_type} {entity_id} in {gard_id}; batches={len(abstract_batches)}")
    entity_desc = "group of authors" if entity_type == "cluster" else "individual author"
    current_expertise: Dict[str, Any] = {}
    for batch_index, batch in enumerate(abstract_batches, start=1):
        batch_content = "\n\n".join(batch)
        if batch_index == 1:
            prompt = (
                f"Given research content from a {entity_desc} working on {gard_id} (a rare disease), identify 3 key detailed research expertise areas.\n\n"
                "Follow these steps:\n"
                "1- Research Contribution Summary: Based on their publications, clinical trials, or grant proposals, summarize the major contributions made by the researcher(s) in the field.\n\n"
                "2- Identify Specific Expertise Areas: Based on their contributions, select 3 highly specific expertise areas, methodologies, or focus topics. "
                "**Avoid broad umbrella terms** Each expertise should be 2–5 words long and domain-specific.\n\n"
                f"Research content (Batch {batch_index}/{len(abstract_batches)}):\n{batch_content}\n\n"
                "Format your response as a dictionary with concise expertise keywords only:\n"
                "{\"expertise_1\": \"concise expertise area\", \"expertise_2\": \"concise expertise area\", \"expertise_3\": \"concise expertise area\"}"
            )
        else:
            prompt = (
                f"You are refining expertise areas for a {entity_desc} working on {gard_id} (a rare disease).\n"
                f"Current expertise areas identified: {json.dumps(current_expertise)}\n\n"
                "Now review additional research content and refine/update the expertise areas. Follow these steps:\n"
                "1- Integration Analysis: Analyze how this new research content relates to or extends the current expertise areas.\n\n"
                "2- Refined Expertise Areas: Update and refine the 3 expertise areas to better represent the complete research profile. "
                "**Avoid broad umbrella terms** Each expertise should be 2–5 words long and domain-specific.\n\n"
                f"Additional research content (Batch {batch_index}/{len(abstract_batches)}):\n{batch_content}\n\n"
                "Format your response as a dictionary with concise expertise keywords only:\n"
                "{\"expertise_1\": \"concise expertise area\", \"expertise_2\": \"concise expertise area\", \"expertise_3\": \"concise expertise area\"}"
            )
        # Retry up to 5 times per batch to obtain strictly valid JSON with 3 non-empty keys
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            outputs = llm.generate([prompt], sampling_params)
            text = outputs[0].outputs[0].text.strip() if outputs and outputs[0].outputs else ""
            parsed = _extract_json_from_text(text)
            if _is_valid_expertise_dict(parsed):
                current_expertise = parsed
                break
            # Strengthen instruction for next attempt
            prompt = (
                prompt + "\n\nReturn EXACTLY a JSON object with keys 'expertise_1','expertise_2','expertise_3' only, each 2-5 words, no lists, no extra keys."
            )
    # Final compact retry across a small sample if still empty after all batches
    if not current_expertise:
        compact_limit = 4000
        acc = 0
        compact_texts: List[str] = []
        for batch in abstract_batches:
            for t in batch:
                if acc + len(t) > compact_limit:
                    break
                compact_texts.append(t)
                acc += len(t)
            if acc >= compact_limit:
                break
        compact_body = "\n\n".join(compact_texts)
        prompt = (
            f"Given research content from a {entity_desc} working on {gard_id} (a rare disease), identify 3 key detailed research expertise areas.\n\n"
            f"Research content (Compact sample):\n{compact_body}\n\n"
            "Format your response as a dictionary with concise expertise keywords only:\n"
            "{\"expertise_1\": \"concise expertise area\", \"expertise_2\": \"concise expertise area\", \"expertise_3\": \"concise expertise area\"}"
        )
        for attempt in range(1, 4):
            outputs = llm.generate([prompt], sampling_params)
            text = outputs[0].outputs[0].text.strip() if outputs and outputs[0].outputs else ""
            parsed = _extract_json_from_text(text)
            if _is_valid_expertise_dict(parsed):
                current_expertise = parsed
                break
            prompt = (
                prompt + "\n\nReturn EXACTLY a JSON object with keys 'expertise_1','expertise_2','expertise_3' only, each 2-5 words, no lists, no extra keys."
            )
    return current_expertise if current_expertise else {"expertise_summary": "insufficient content"}


def _gard_id_from_clustered_filename(path: str) -> str:
    base = os.path.basename(path)
    if base.startswith('clustered_') and base.endswith('.csv'):
        return base[len('clustered_'):-4].replace('_', ':')
    return ""


def _append_expertise_row(expertise_file: str, row: Dict[str, Any]) -> None:
    pd.DataFrame([row])[['entity_type', 'entity_id', 'gard_id', 'cluster_label', 'expertise', 'response']].to_csv(
        expertise_file, mode='a', header=False, index=False
    )


def _append_cluster_row_if_new(cluster_file: str, processed_clusters: set, gard_id: str, cluster_label: int, num_authors: int) -> None:
    key = (gard_id, cluster_label)
    if key in processed_clusters:
        return
    processed_clusters.add(key)
    out = {
        'cluster_id': f"{gard_id.replace(':', '_')}_cluster_{cluster_label}",
        'gard_id': gard_id,
        'cluster_label': cluster_label,
        'num_authors': num_authors,
    }
    pd.DataFrame([out])[['cluster_id', 'gard_id', 'cluster_label', 'num_authors']].to_csv(
        cluster_file, mode='a', header=False, index=False
    )


def _append_relationship_rows(rel_file: str, entity_id: str, cluster_id: str, gard_id: str, cluster_label: int, author_ids: List[Any], relationship_type: str) -> None:
    rows = []
    for a_id in author_ids:
        rows.append({
            'entity_id': entity_id,
            'cluster_id': cluster_id,
            'gard_id': gard_id,
            'cluster_label': cluster_label,
            'author_id': a_id,
            'relationship_type': relationship_type,
        })
    if rows:
        pd.DataFrame(rows)[['entity_id', 'cluster_id', 'gard_id', 'cluster_label', 'author_id', 'relationship_type']].to_csv(
            rel_file, mode='a', header=False, index=False
        )


def process_single_gard_file(
    clustered_file: str,
    author_abstracts: Dict[Any, List[Dict[str, Any]]],
    llm,
    sampling_params,
    max_batch_size: int,
    expertise_file: str,
    cluster_file: str,
    rel_file: str,
    processed_clusters: set,
    pbar=None,
) -> Tuple[int, int]:
    gard_id = _gard_id_from_clustered_filename(clustered_file)
    if not gard_id:
        print(f"Skipping file with unrecognized name: {clustered_file}")
        return 0, 0
    print(f"Processing {gard_id}")
    df = pd.read_csv(clustered_file)
    num_clusters_written = 0
    num_individuals_written = 0

    # Clusters (exclude noise) - stream writes per cluster
    cluster_groups = df[df['cluster'] != -1].groupby('cluster')
    for cluster_label, group in cluster_groups:
        cluster_entity_id = f"{gard_id.replace(':', '_')}_cluster_{int(cluster_label)}"
        author_ids = group['author_id'].tolist()
        abstracts_text: List[str] = []
        for a_id in author_ids:
            fa = author_abstracts.get(a_id)
            if fa:
                abstracts_text.extend(extract_abstract_content(fa))
        if not abstracts_text:
            continue
        batches = split_abstracts_into_batches(abstracts_text, max_batch_size=max_batch_size)
        expertise = generate_iterative_expertise_summary(author_ids, batches, gard_id, "cluster", cluster_entity_id, llm, sampling_params)
        # Write expertise row immediately
        _append_expertise_row(expertise_file, {
            'entity_type': 'cluster',
            'entity_id': cluster_entity_id,
            'gard_id': gard_id,
            'cluster_label': int(cluster_label),
            'expertise': json.dumps(expertise),
            'response': "",
        })
        # Write/ensure cluster node
        _append_cluster_row_if_new(cluster_file, processed_clusters, gard_id, int(cluster_label), len(author_ids))
        # Write relationships
        _append_relationship_rows(rel_file, cluster_entity_id, cluster_entity_id, gard_id, int(cluster_label), author_ids, 'CLUSTER_EXPERTISE')
        num_clusters_written += 1
        print(f"  Wrote cluster {cluster_label} ({len(author_ids)} authors)")
        if pbar is not None:
            try:
                pbar.update(1)
                # Periodic newline progress for SLURM logs (every 100 entities)
                if pbar.n % 100 == 0:
                    print(f"Progress: {pbar.n}/{pbar.total if pbar.total else '?'} entities")
            except Exception:
                pass

    # Noise (individuals) - stream writes per author
    noise_df = df[df['cluster'] == -1]
    for _, row in noise_df.iterrows():
        a_id = row['author_id']
        abstracts = extract_abstract_content(author_abstracts.get(a_id, []))
        if not abstracts:
            continue
        entity_id = f"{gard_id.replace(':', '_')}_noise_{a_id}"
        batches = split_abstracts_into_batches(abstracts, max_batch_size=max_batch_size)
        expertise = generate_iterative_expertise_summary([a_id], batches, gard_id, "individual", entity_id, llm, sampling_params)
        # Write expertise row
        _append_expertise_row(expertise_file, {
            'entity_type': 'individual',
            'entity_id': entity_id,
            'gard_id': gard_id,
            'cluster_label': -1,
            'expertise': json.dumps(expertise),
            'response': "",
        })
        # Relationships for individual (no cluster_id)
        _append_relationship_rows(rel_file, entity_id, "", gard_id, -1, [a_id], 'INDIVIDUAL_EXPERTISE')
        num_individuals_written += 1
        print(f"  Wrote individual {a_id}")
        if pbar is not None:
            try:
                pbar.update(1)
                if pbar.n % 100 == 0:
                    print(f"Progress: {pbar.n}/{pbar.total if pbar.total else '?'} entities")
            except Exception:
                pass

    return num_clusters_written, num_individuals_written


def generate_expertise_information(input_dir: str, output_dir: str, step1_file: str, model_path: str, tensor_parallel_size: int, gpu_memory_utilization: float, max_model_len: int, max_batch_size: int):
    print("Generating expertise nodes and relationships...")
    os.makedirs(output_dir, exist_ok=True)

    author_abstracts = load_step1_author_data(step1_file)
    clustered_files = glob.glob(os.path.join(input_dir, "clustered_*.csv"))
    print(f"Found {len(clustered_files)} clustered files")
    if not clustered_files:
        print("No clustered files found; exiting.")
        return None

    llm, sampling_params = initialize_llm(model_path, tensor_parallel_size, gpu_memory_utilization, max_model_len)

    expertise_file = os.path.join(output_dir, "expertise_node.csv")
    cluster_file = os.path.join(output_dir, "cluster_node.csv")
    rel_file = os.path.join(output_dir, "expertise_relationships.csv")

    # Initialize headers
    pd.DataFrame(columns=['entity_type', 'entity_id', 'gard_id', 'cluster_label', 'expertise', 'response']).to_csv(expertise_file, index=False)
    pd.DataFrame(columns=['cluster_id', 'gard_id', 'cluster_label', 'num_authors']).to_csv(cluster_file, index=False)
    pd.DataFrame(columns=['entity_id', 'cluster_id', 'gard_id', 'cluster_label', 'author_id', 'relationship_type']).to_csv(rel_file, index=False)

    processed_clusters = set()

    total_clusters = 0
    total_individuals = 0

    # Pre-count total entities for a global progress bar
    total_entities = 0
    for _file in clustered_files:
        try:
            _df = pd.read_csv(_file, usecols=["cluster"])
            num_clusters = _df.loc[_df['cluster'] != -1, 'cluster'].nunique()
            num_individuals = int((_df['cluster'] == -1).sum())
            total_entities += int(num_clusters + num_individuals)
        except Exception:
            pass
    if total_entities <= 0:
        total_entities = None

    with tqdm(total=total_entities, desc="Generating expertise", unit="entity", ascii=True, dynamic_ncols=False, mininterval=5, smoothing=0) as pbar:
        for clustered_file in tqdm(clustered_files, desc="Processing GARD files"):
            try:
                c_count, i_count = process_single_gard_file(
                    clustered_file,
                    author_abstracts,
                    llm,
                    sampling_params,
                    max_batch_size=max_batch_size,
                    expertise_file=expertise_file,
                    cluster_file=cluster_file,
                    rel_file=rel_file,
                    processed_clusters=processed_clusters,
                    pbar=pbar,
                )
                total_clusters += c_count
                total_individuals += i_count
            except Exception as e:
                print(f"  Error processing {clustered_file}: {e}")

    print(f"Saved expertise nodes to {expertise_file}")
    print(f"Saved cluster nodes to {cluster_file}")
    print(f"Saved relationships to {rel_file}")
    print(f"Totals written: clusters={total_clusters}, individuals={total_individuals}")
    return expertise_file, cluster_file, rel_file


def main():
    parser = argparse.ArgumentParser(description="Generate expertise nodes from clustered authors")
    parser.add_argument("--input_dir", type=str, default="step2_clustering/clustering", help="Directory containing clustered_*.csv from step 2")
    parser.add_argument("--output_dir", type=str, default="step2_clustering/expertise", help="Directory to write expertise CSVs")
    parser.add_argument("--step1_file", type=str, default="step2_clustering/author_information.json", help="Path to author_information.json (with full_abstract)")
    parser.add_argument("--model_path", type=str, default=os.getenv("EXPERTISE_MODEL_PATH"), help="Path to LLM model for vLLM")
    parser.add_argument("--tensor_parallel_size", type=int, default=4, help="Tensor parallelism for vLLM")
    parser.add_argument("--gpu_memory_utilization", type=float, default=0.95, help="GPU memory utilization for vLLM")
    parser.add_argument("--max_model_len", type=int, default=8192, help="Max context length for model")
    parser.add_argument("--max_batch_size", type=int, default=20000, help="Max characters per LLM batch of abstracts")

    args = parser.parse_args()
    if not args.model_path:
        parser.error("Set --model_path or EXPERTISE_MODEL_PATH")

    generate_expertise_information(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        step1_file=args.step1_file,
        model_path=args.model_path,
        tensor_parallel_size=args.tensor_parallel_size,
        gpu_memory_utilization=args.gpu_memory_utilization,
        max_model_len=args.max_model_len,
        max_batch_size=args.max_batch_size,
    )


if __name__ == "__main__":
    main()
