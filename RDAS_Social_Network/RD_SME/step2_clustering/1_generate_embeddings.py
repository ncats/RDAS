"""
Step 2 (current): Generate author embeddings for clustering

This script ingests author_information.json (per-author background with
full_abstract items) and generates a vector per author by encoding the
concatenated text of all titles+abstracts in full_abstract.

Features combined from v1 and v2:
- GPU acceleration (SentenceTransformer or BioBERT)
- L2 normalization and optional UMAP
- Optional memory-safe streaming mode (ijson) for large inputs

Default input points to the generated author_information.json for GARD:0006233.
Outputs a CSV of embeddings per GARD group and a summary CSV.
"""

import os
import math
import argparse
import json
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool

import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel
from sklearn.preprocessing import normalize
from umap.umap_ import UMAP

# Optional: streaming for very large JSON files
try:
    import ijson  # type: ignore
except Exception:
    ijson = None  # Streaming disabled if unavailable


# Global model state for workers
model = None
tokenizer = None
model_type = None


def init_worker(chosen_model_type: str) -> None:
    global model, tokenizer, model_type
    model_type = chosen_model_type
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    print(f"Using device: {device}")

    if model_type == 'sentence-transformer':
        print("Initializing SentenceTransformer model in worker process...")
        model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
        tokenizer = None
    elif model_type == 'biobert':
        print("Initializing BioBERT model in worker process...")
        tokenizer = AutoTokenizer.from_pretrained('dmis-lab/biobert-base-cased-v1.1')
        model_local = AutoModel.from_pretrained('dmis-lab/biobert-base-cased-v1.1')
        model = model_local.to(device)
        model.eval()


def extract_text_from_abstract(full_abstract: Any) -> str:
    """Concatenate title + abstract for each item in the list."""
    if not full_abstract or not isinstance(full_abstract, list):
        return ""
    parts: List[str] = []
    for item in full_abstract:
        if isinstance(item, dict):
            title = item.get('title', '')
            abstract = item.get('abstract', '')
            combined = ''
            if title and str(title).strip():
                combined += str(title).strip()
            if abstract and str(abstract).strip():
                if combined:
                    combined += ' '
                combined += str(abstract).strip()
            if combined:
                parts.append(combined)
    return ' '.join(parts)


def get_embeddings(texts: List[str], biobert_batch_size: int = 32, st_batch_size: Optional[int] = None) -> np.ndarray:
    global model, tokenizer, model_type
    device = 'cuda' if torch.cuda.is_available() else 'cpu'

    if model_type == 'sentence-transformer':
        if st_batch_size is not None and st_batch_size > 0:
            return model.encode(texts, show_progress_bar=False, batch_size=st_batch_size)
        return model.encode(texts, show_progress_bar=False)
    elif model_type == 'biobert':
        batch_size = max(1, int(biobert_batch_size))
        all_embeddings: List[np.ndarray] = []
        total_batches = math.ceil(len(texts) / batch_size) if texts else 0
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            inputs = tokenizer(batch_texts, return_tensors='pt', truncation=True, padding=True, max_length=512)
            inputs = {k: v.to(device) for k, v in inputs.items()}
            with torch.no_grad():
                outputs = model(**inputs)
                cls_embeddings = outputs.last_hidden_state[:, 0, :].cpu().numpy()
                all_embeddings.append(cls_embeddings)
            # Progress logging every 50 batches
            batch_index = (i // batch_size) + 1
            if batch_index % 50 == 0 or batch_index == total_batches:
                print(f"    BioBERT batches: {batch_index}/{total_batches}")
        return np.vstack(all_embeddings) if all_embeddings else np.zeros((0, 768), dtype=np.float32)
    else:
        raise ValueError(f"Unsupported model_type: {model_type}")


def _process_group(gard_id: str, group_df: pd.DataFrame, output_path: str, use_l2_norm: bool, use_umap: bool, umap_components: int, biobert_batch_size: int = 32, st_batch_size: Optional[int] = None) -> Optional[Dict[str, Any]]:
    try:
        print(f"Processing GARD ID: {gard_id} with {len(group_df)} authors")

        texts: List[str] = []
        for fa in group_df['full_abstract'].tolist():
            texts.append(extract_text_from_abstract(fa))

        if not texts or all(not t.strip() for t in texts):
            print(f"  Skipping {gard_id}: No meaningful content")
            return None

        embeddings = get_embeddings(texts, biobert_batch_size=biobert_batch_size, st_batch_size=st_batch_size)

        if use_l2_norm:
            embeddings = normalize(embeddings, norm='l2')
            print("  Applied L2 normalization")

        min_samples_for_umap = umap_components + 1
        if use_umap and embeddings.shape[0] >= min_samples_for_umap:
            reducer = UMAP(n_components=umap_components, random_state=42)
            embeddings = reducer.fit_transform(embeddings)
            print(f"  Applied UMAP: {embeddings.shape[1]} dimensions -> {umap_components} dimensions")
        elif use_umap:
            print(f"  Skipping UMAP: Not enough samples ({embeddings.shape[0]} < {min_samples_for_umap} required)")

        n_features = embeddings.shape[1]
        feature_cols = [f'feature_{i}' for i in range(n_features)]
        embeddings_df = pd.DataFrame(embeddings, columns=feature_cols)
        embeddings_df['author_id'] = group_df['author_id'].values
        if 'author_name' in group_df.columns:
            embeddings_df['author_name'] = group_df['author_name'].values

        os.makedirs(output_path, exist_ok=True)
        output_file = f"{output_path}/embeddings_{gard_id.replace(':', '_')}.csv"
        embeddings_df.to_csv(output_file, index=False)

        print(f"  Saved embeddings for {gard_id} with {len(embeddings_df)} authors and {n_features} features")
        return {
            'gard_id': gard_id,
            'num_authors': len(embeddings_df),
            'num_features': n_features,
            'embeddings_file': output_file,
        }
    except Exception as e:
        print(f"  Error processing {gard_id}: {str(e)}")
        return None


def _process_group_from_args(args_tuple) -> Optional[Dict[str, Any]]:
    gard_id, group_df, output_path, use_l2_norm, use_umap, umap_components, biobert_batch_size, st_batch_size = args_tuple
    return _process_group(gard_id, group_df, output_path, use_l2_norm, use_umap, umap_components, biobert_batch_size, st_batch_size)


def process_author_embeddings(input_file: str, output_path: str, n_processes: Optional[int], chosen_model_type: str, use_l2_norm: bool, use_umap: bool, umap_components: int, biobert_batch_size: int = 32, st_batch_size: Optional[int] = None) -> Tuple[pd.DataFrame, str]:
    global model, tokenizer, model_type

    features: List[str] = []
    if use_l2_norm:
        features.append("L2 normalization")
    if use_umap:
        features.append(f"UMAP (n_components={umap_components})")
    suffix = f" with {', '.join(features)}" if features else ""
    print(f"Processing author embeddings from {input_file} using {chosen_model_type}{suffix}")

    os.makedirs(output_path, exist_ok=True)

    with open(input_file, 'r', encoding='utf-8') as f:
        author_data = json.load(f)

    print(f"Loaded {len(author_data)} author records")
    df = pd.DataFrame(author_data)

    # Ensure minimal columns exist
    for col in ['author_id', 'full_abstract']:
        if col not in df.columns:
            df[col] = None

    # Filter authors with any GardId list values
    df = df[df['GardId'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
    print(f"Filtered to {len(df)} authors with non-empty GARD IDs")

    exploded_df = df.explode('GardId')
    exploded_df = exploded_df[exploded_df['GardId'].notna() & (exploded_df['GardId'] != '') & (exploded_df['GardId'] != 'nan')]

    gard_groups = exploded_df.groupby('GardId')
    num_groups = len(gard_groups)
    print(f"Found {num_groups} GARD groups")

    # Avoid multiprocessing when there is only one group (prevents heavy GPU model init in many workers)
    if n_processes and n_processes > 1 and num_groups <= 1:
        print("Only 1 GARD group detected; forcing sequential execution to avoid GPU contention.")
        n_processes = 1

    # Initialize model for sequential path
    model = None
    tokenizer = None
    model_type = None

    results: List[Dict[str, Any]] = []
    if n_processes and n_processes > 1:
        args_list = [(gard_id, group, output_path, use_l2_norm, use_umap, umap_components, biobert_batch_size, st_batch_size) for gard_id, group in gard_groups]
        with Pool(processes=n_processes, initializer=init_worker, initargs=(chosen_model_type,)) as pool:
            for result in tqdm(pool.imap_unordered(_process_group_from_args, args_list), total=len(args_list), desc="Processing GARD groups"):
                if result:
                    results.append(result)
    else:
        # Sequential: init model here
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        print(f"Using device: {device}")
        model_type = chosen_model_type
        if model_type == 'sentence-transformer':
            print("Initializing SentenceTransformer model for sequential processing...")
            model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
            tokenizer = None
        elif model_type == 'biobert':
            print("Initializing BioBERT model for sequential processing...")
            tokenizer = AutoTokenizer.from_pretrained('dmis-lab/biobert-base-cased-v1.1')
            model_local = AutoModel.from_pretrained('dmis-lab/biobert-base-cased-v1.1')
            model = model_local.to(device)
            model.eval()

        for gard_id, group in tqdm(gard_groups, desc="Processing GARD groups"):
            result = _process_group(gard_id, group, output_path, use_l2_norm, use_umap, umap_components, biobert_batch_size=biobert_batch_size, st_batch_size=st_batch_size)
            if result:
                results.append(result)

    summary_df = pd.DataFrame(results)
    summary_file = f"{output_path}/gard_embeddings_summary.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"Processed {len(summary_df)} GARD groups successfully")
    print(f"Saved summary to {summary_file}")
    return summary_df, summary_file


def _safe_gard_filename(gard_id: str) -> str:
    return f"gard_{gard_id.replace(':', '_')}.jsonl"


def _extract_minimal_author_record(author_obj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'author_id': author_obj.get('author_id'),
        'author_name': author_obj.get('author_name'),
        'full_abstract': author_obj.get('full_abstract'),
        'GardId': author_obj.get('GardId'),
    }


def _stream_input_to_temp_files(input_file: str, temp_dir: str, max_open_files: int = 64) -> Dict[str, str]:
    if ijson is None:
        raise RuntimeError("ijson is required for streaming mode but is not installed.")
    from collections import OrderedDict

    os.makedirs(temp_dir, exist_ok=True)
    gard_to_path: Dict[str, str] = {}
    open_files: "OrderedDict[str, Any]" = OrderedDict()

    def get_handle(safe_name: str, path: str):
        nonlocal open_files
        if safe_name in open_files:
            fh = open_files.pop(safe_name)
            open_files[safe_name] = fh
            return fh
        fh = open(path, 'a', encoding='utf-8')
        open_files[safe_name] = fh
        if len(open_files) > max_open_files:
            old_safe, old_fh = open_files.popitem(last=False)
            try:
                old_fh.close()
            except Exception:
                pass
        return fh

    with open(input_file, 'r', encoding='utf-8') as f:
        for author in ijson.items(f, 'item'):
            if not isinstance(author, dict):
                continue
            gard_ids = author.get('GardId')
            if not isinstance(gard_ids, list) or not gard_ids:
                continue
            minimal = _extract_minimal_author_record(author)
            for gard_id in gard_ids:
                if not gard_id or not isinstance(gard_id, str):
                    continue
                safe = _safe_gard_filename(gard_id)
                path = os.path.join(temp_dir, safe)
                gard_to_path[gard_id] = path
                fh = get_handle(safe, path)
                fh.write(json.dumps(minimal) + '\n')

    for _, fh in list(open_files.items()):
        try:
            fh.close()
        except Exception:
            pass

    return gard_to_path


def _read_jsonl(path: str) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                obj = json.loads(line)
                records.append(obj)
            except Exception:
                continue
    if not records:
        return pd.DataFrame(columns=['author_id', 'author_name', 'full_abstract'])
    return pd.DataFrame(records)


def process_author_embeddings_streaming(input_file: str, output_path: str, chosen_model_type: str, use_l2_norm: bool, use_umap: bool, umap_components: int, n_processes: int = 1, temp_dir: Optional[str] = None, biobert_batch_size: int = 32, st_batch_size: Optional[int] = None) -> Tuple[pd.DataFrame, str]:
    print(f"Streaming mode enabled. Input: {input_file}")
    os.makedirs(output_path, exist_ok=True)
    if temp_dir is None:
        temp_dir = os.path.join(output_path, "_tmp_step2_chunks")
    os.makedirs(temp_dir, exist_ok=True)

    gard_to_path = _stream_input_to_temp_files(input_file, temp_dir)
    print(f"Prepared per-GARD temporary files: {len(gard_to_path)} GARD groups")

    global model, tokenizer, model_type
    # Sequential in streaming mode to minimize memory
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    print(f"Using device: {device}")
    model_type = chosen_model_type
    if model_type == 'sentence-transformer':
        print("Initializing SentenceTransformer model for sequential processing...")
        model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
        tokenizer = None
    elif model_type == 'biobert':
        print("Initializing BioBERT model for sequential processing...")
        tokenizer = AutoTokenizer.from_pretrained('dmis-lab/biobert-base-cased-v1.1')
        model_local = AutoModel.from_pretrained('dmis-lab/biobert-base-cased-v1.1')
        model = model_local.to(device)
        model.eval()

    results: List[Dict[str, Any]] = []
    for gard_id, path in tqdm(gard_to_path.items(), desc="Processing GARD groups (streaming)"):
        group_df = _read_jsonl(path)
        if group_df.empty:
            continue
        result = _process_group(gard_id, group_df, output_path, use_l2_norm, use_umap, umap_components, biobert_batch_size=biobert_batch_size, st_batch_size=st_batch_size)
        if result:
            results.append(result)

    summary_df = pd.DataFrame(results)
    summary_file = f"{output_path}/gard_embeddings_summary.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"Processed {len(summary_df)} GARD groups successfully (streaming mode)")
    print(f"Saved summary to {summary_file}")
    return summary_df, summary_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate author embeddings for clustering")
    parser.add_argument("--input", type=str, default="step2_clustering/author_information.json", help="Path to author_information.json")
    parser.add_argument("--output", type=str, default="step2_clustering/embeddings", help="Output directory for embeddings")
    parser.add_argument("--processes", type=int, default=None, help="Number of processes for parallel execution (default: CPU count - 1)")
    parser.add_argument("--model", type=str, choices=['sentence-transformer', 'biobert'], default='sentence-transformer', help="Model type to use for embeddings")
    parser.add_argument("--l2-norm", action='store_true', help="Apply L2 normalization to embeddings")
    parser.add_argument("--umap", action='store_true', help="Apply UMAP dimensionality reduction")
    parser.add_argument("--umap-components", type=int, default=50, help="Number of UMAP components (default: 50)")
    parser.add_argument("--streaming", action='store_true', help="Enable memory-safe streaming mode")
    parser.add_argument("--temp-dir", type=str, default=None, help="Temporary directory for streaming chunks")
    parser.add_argument("--biobert-batch-size", type=int, default=32, help="Batch size for BioBERT (default: 32)")
    parser.add_argument("--st-batch-size", type=int, default=None, help="Batch size for SentenceTransformer (default: library default)")

    args = parser.parse_args()

    if args.processes is None:
        args.processes = max(1, os.cpu_count() - 1)

    if args.streaming:
        process_author_embeddings_streaming(
            input_file=args.input,
            output_path=args.output,
            chosen_model_type=args.model,
            use_l2_norm=args.l2_norm,
            use_umap=args.umap,
            umap_components=args.umap_components,
            n_processes=args.processes,
            temp_dir=args.temp_dir,
            biobert_batch_size=args.biobert_batch_size,
            st_batch_size=args.st_batch_size,
        )
    else:
        process_author_embeddings(
            input_file=args.input,
            output_path=args.output,
            n_processes=args.processes,
            chosen_model_type=args.model,
            use_l2_norm=args.l2_norm,
            use_umap=args.umap,
            umap_components=args.umap_components,
            biobert_batch_size=args.biobert_batch_size,
            st_batch_size=args.st_batch_size,
        )


if __name__ == "__main__":
    main()
