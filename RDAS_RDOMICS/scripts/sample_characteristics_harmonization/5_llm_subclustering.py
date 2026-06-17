#!/usr/bin/env python3
"""
LLM-based Sub-clustering within Sample Characteristic Categories

This script takes the clustered results from step 4 and further sub-clusters 
the labels within each major category into more specific sub-categories.

INPUT FILES:
-----------
1. clustered_sample_characteristics.csv - Main categories from step 4

OUTPUT:
-------
Configured subclustering output CSV - Sub-categories results
"""

import pandas as pd
import json
import numpy as np
from typing import List, Dict, Set, Tuple
import os
import sys
from pathlib import Path
from collections import defaultdict, Counter
import re
import time
import ast
from difflib import SequenceMatcher

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils import ensure_parent_dir, load_paths, load_settings

# Import vLLM for local LLM inference
try:
    from vllm import LLM, SamplingParams
    VLLM_AVAILABLE = True
except ImportError:
    print("Warning: vLLM not available. Please install vLLM.")
    VLLM_AVAILABLE = False
    sys.exit(1)


# ------------------------- Data Loading Helpers -------------------------

def initialize_llm(model_path: str):
    """Initialize the Llama3 model using vLLM."""
    try:
        print("Initializing Llama3 model for sub-clustering...")
        llm = LLM(
            model=model_path,
            tensor_parallel_size=4,
            gpu_memory_utilization=0.95,
            max_model_len=8192
        )
        
        sampling_params = SamplingParams(
            temperature=0.02,  # Very low temperature for consistent results
            max_tokens=4096,
            top_p=0.9,
            stop=["</s>", "\n\n\n"]
        )
        
        print("✓ Llama3 model initialized successfully")
        return llm, sampling_params
        
    except Exception as e:
        print(f"✗ Error initializing LLM: {e}")
        return None, None


def load_clustered_data(clustered_file: str) -> Dict[str, List[str]]:
    """Load clustered data and extract labels for each category."""
    
    print(f"Loading clustered data from {clustered_file}...")
    clustered_df = pd.read_csv(clustered_file)
    
    # Process clustered data
    categories_data = {}
    for _, row in clustered_df.iterrows():
        category = row['standardized_term']
        
        # Parse labels list from string representation
        try:
            labels_list = ast.literal_eval(row['labels'])
        except:
            # Fallback parsing if ast fails
            labels_str = row['labels'].strip("[]'\"")
            labels_list = [label.strip().strip("'\"") for label in labels_str.split("', '")]
        
        categories_data[category] = labels_list
    
    print(f"✓ Loaded data for {len(categories_data)} categories from clustered file")
    
    return categories_data


# ------------------------- LLM Prompting -------------------------

def create_subclustering_prompt(category_name: str, labels_batch: List[str], 
                               max_subcategories: int = 8) -> str:
    """Create a sub-clustering prompt for the LLM."""
    
    # Format labels for display
    labels_text = ""
    for i, label in enumerate(labels_batch, 1):
        labels_text += f"{i}. \"{label}\"\n"
    
    prompt = f"""You are an expert biomedical data curator. Your task is to sub-cluster these "{category_name}" labels into more specific, meaningful sub-categories.

LABELS TO SUB-CLUSTER:
{labels_text}

INSTRUCTIONS:
- Create EXACTLY {max_subcategories} or fewer meaningful sub-categories that best organize these labels
- Each sub-category should represent a coherent biological or technical concept
- Sub-category names should be concise, descriptive, and not include the main category name
- Prefer broader categories that can accommodate multiple related labels
- Avoid creating sub-categories with only 1-2 labels - merge them into broader categories
- IMPORTANT: Assign EVERY label to one of your named sub-categories. Do NOT output an "Other" sub-category.

OUTPUT FORMAT - Respond with ONLY valid JSON (no explanations):
{{
    "subcategories": [
        {{
            "name": "Sub-category Name 1",
            "labels": ["label1", "label2", "label3"]
        }},
        {{
            "name": "Sub-category Name 2", 
            "labels": ["label4", "label5", "label6"]
        }}
    ]
}}

JSON:"""
    
    return prompt


def extract_json_from_response(response_text: str) -> Dict:
    """Extract JSON from LLM response with robust error handling and sanitization."""
    raw = response_text.strip()

    def try_load(s: str) -> Dict:
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and 'subcategories' in obj and isinstance(obj['subcategories'], list):
                return obj
        except Exception:
            pass
        return None

    # 1) Remove code fences if present
    if raw.startswith('```'):
        raw = raw.strip('`')
        # Remove possible leading language tag like json
        if raw.startswith('json'):
            raw = raw[4:]
    raw = raw.strip()

    # 2) Extract the most outer JSON object
    if '{' in raw and '}' in raw:
        start_idx = raw.find('{')
        end_idx = raw.rfind('}') + 1
        candidate = raw[start_idx:end_idx]
        result = try_load(candidate)
        if result:
            return result

    # 3) If prefixed with JSON: marker, try that part specifically
    if 'JSON:' in raw:
        json_part = raw.split('JSON:')[-1].strip()
        if '{' in json_part and '}' in json_part:
            start_idx = json_part.find('{')
            end_idx = json_part.rfind('}') + 1
            candidate = json_part[start_idx:end_idx]
            result = try_load(candidate)
            if result:
                return result

    # 4) Sanitization: remove trailing commas before ] or }
    sanitized = re.sub(r',\s*([}\]])', r'\1', raw)
    if '{' in sanitized and '}' in sanitized:
        start_idx = sanitized.find('{')
        end_idx = sanitized.rfind('}') + 1
        candidate = sanitized[start_idx:end_idx]
        result = try_load(candidate)
        if result:
            return result

    # 5) Last resort: attempt Python literal eval after converting JSON booleans/null
    pyish = raw.replace('true', 'True').replace('false', 'False').replace('null', 'None')
    if '{' in pyish and '}' in pyish:
        start_idx = pyish.find('{')
        end_idx = pyish.rfind('}') + 1
        candidate = pyish[start_idx:end_idx]
        try:
            obj = ast.literal_eval(candidate)
            if isinstance(obj, dict) and 'subcategories' in obj and isinstance(obj['subcategories'], list):
                return obj
        except Exception:
            pass

    return None


# ------------------------- LLM Subclustering -------------------------

def create_reassignment_prompt(category_name: str, existing_subcats: List[Dict], leftovers: List[str]) -> str:
    """Prompt that asks the LLM to assign leftover labels to one of the existing subcategory names."""
    names = [sc['name'] for sc in existing_subcats]
    # Build small examples for each subcategory to guide assignment
    examples_text = ""
    for sc in existing_subcats:
        sample_labels = sc['labels'][:5]
        examples_text += f"- {sc['name']}: {sample_labels}\n"
    leftovers_text = "\n".join([f"- {l}" for l in leftovers])
    prompt = f"""You are assisting with finalizing sub-categories for the main category "{category_name}".
We already have the following sub-categories:
{names}

EXAMPLES (few labels assigned to each sub-category):
{examples_text}

Please assign EACH of the following leftover labels to ONE of the existing sub-category names above.
Return ONLY valid JSON with this format (no explanations):
{{
  "assignments": [
    {{"label": "leftover_label_1", "subcategory": "One of {names}"}},
    {{"label": "leftover_label_2", "subcategory": "One of {names}"}}
  ]
}}

LEFTOVER LABELS:
{leftovers_text}

JSON:"""
    return prompt


def llm_reassign_leftovers(category_name: str, existing_subcats: List[Dict], leftovers: List[str], llm, sampling_params) -> Dict[str, str]:
    """Ask the LLM to map each leftover label to one of the existing subcategory names."""
    if not leftovers:
        return {}
    prompt = create_reassignment_prompt(category_name, existing_subcats, leftovers)
    try:
        outputs = llm.generate([prompt], sampling_params)
        response_text = outputs[0].outputs[0].text.strip()
        # Extract simple JSON
        try:
            data = json.loads(response_text[response_text.find('{'):response_text.rfind('}')+1])
        except Exception:
            data = None
        mapping: Dict[str, str] = {}
        if data and isinstance(data.get('assignments'), list):
            valid_names = {sc['name'] for sc in existing_subcats}
            for item in data['assignments']:
                label = item.get('label')
                subcat = item.get('subcategory')
                if label in leftovers and subcat in valid_names:
                    mapping[label] = subcat
        return mapping
    except Exception as e:
        print(f"✗ Error in leftover reassignment: {e}")
        return {}


def heuristic_reassign_leftovers(existing_subcats: List[Dict], leftovers: List[str]) -> Dict[str, str]:
    """Heuristic fallback: assign each leftover to the most similar existing subcategory (name or labels)."""
    mapping: Dict[str, str] = {}
    for label in leftovers:
        best_name = None
        best_score = -1.0
        for sc in existing_subcats:
            # Compare to subcategory name
            score = SequenceMatcher(None, label.lower(), sc['name'].lower()).ratio()
            # Also compare to a few labels in the subcategory
            for ex in sc['labels'][:5]:
                score = max(score, SequenceMatcher(None, label.lower(), ex.lower()).ratio())
            if score > best_score:
                best_score = score
                best_name = sc['name']
        if best_name is not None:
            mapping[label] = best_name
    return mapping


def llm_subclustering(category_name: str, labels_batch: List[str], 
                     llm, sampling_params, max_subcategories: int = 8, retry_depth: int = 0) -> Dict:
    """Perform LLM-based sub-clustering with retry logic (smaller batches on failure)."""
    
    prompt = create_subclustering_prompt(category_name, labels_batch, max_subcategories)
    
    try:
        outputs = llm.generate([prompt], sampling_params)
        response_text = outputs[0].outputs[0].text.strip()
        
        print(f"LLM Response preview: {response_text[:200]}...")
        
        result = extract_json_from_response(response_text)
        
        if result and 'subcategories' in result:
            # Validate subcategories
            valid_subcategories = []
            all_input_labels = set(labels_batch)
            assigned_labels = set()
            
            for subcat in result['subcategories']:
                if 'name' in subcat and 'labels' in subcat:
                    # Filter to only include labels that were in the input
                    clean_labels = [label for label in subcat['labels'] if label in all_input_labels]
                    if clean_labels:
                        # De-duplicate labels within subcategory
                        seen_local = set()
                        deduped = []
                        for l in clean_labels:
                            if l not in seen_local:
                                seen_local.add(l)
                                deduped.append(l)
                        assigned_labels.update(deduped)
                        valid_subcategories.append({'name': subcat['name'], 'labels': deduped})
            
            # Handle any unassigned labels by asking LLM to reassign to existing subcategories
            unassigned = [l for l in labels_batch if l not in assigned_labels]
            if unassigned:
                mapping = llm_reassign_leftovers(category_name, valid_subcategories, unassigned, llm, sampling_params)
                # Fallback heuristic if needed
                if not mapping or len(mapping) < len(unassigned):
                    remaining = [l for l in unassigned if l not in mapping]
                    heuristic_map = heuristic_reassign_leftovers(valid_subcategories, remaining)
                    mapping.update(heuristic_map)
                # Apply mapping
                name_to_idx = {sc['name']: i for i, sc in enumerate(valid_subcategories)}
                for label, subname in mapping.items():
                    idx = name_to_idx.get(subname)
                    if idx is not None:
                        valid_subcategories[idx]['labels'].append(label)
                        assigned_labels.add(label)
            
            # Final safety: any still-unassigned go to best-matching existing
            still_unassigned = [l for l in labels_batch if l not in assigned_labels]
            if still_unassigned and valid_subcategories:
                heuristic_map = heuristic_reassign_leftovers(valid_subcategories, still_unassigned)
                name_to_idx = {sc['name']: i for i, sc in enumerate(valid_subcategories)}
                for label, subname in heuristic_map.items():
                    idx = name_to_idx.get(subname)
                    if idx is not None:
                        valid_subcategories[idx]['labels'].append(label)
                        assigned_labels.add(label)
            
            print(f"✓ Successfully created {len(valid_subcategories)} subcategories")
            return {'subcategories': valid_subcategories}
        
        else:
            print("✗ No valid JSON found in LLM response")
            # Retry by splitting into smaller batches once
            if retry_depth == 0 and len(labels_batch) > 20:
                mid = len(labels_batch) // 2
                left = llm_subclustering(category_name, labels_batch[:mid], llm, sampling_params, max_subcategories=max_subcategories, retry_depth=1)
                right = llm_subclustering(category_name, labels_batch[mid:], llm, sampling_params, max_subcategories=max_subcategories, retry_depth=1)
                combined: List[Dict] = []
                for part in [left, right]:
                    if part and 'subcategories' in part:
                        combined.extend(part['subcategories'])
                if combined:
                    return {'subcategories': combined}
            # Fallback: single bucket
            return {'subcategories': [{'name': 'Cluster 1', 'labels': labels_batch}]}
            
    except Exception as e:
        print(f"✗ Error in LLM subclustering: {e}")
        # Fallback: single bucket
        return {'subcategories': [{'name': 'Cluster 1', 'labels': labels_batch}]}


def create_final_merge_prompt(category_name: str, labels: List[str], target_count: int = 8) -> str:
    """Create a prompt for final LLM-based consolidation into representative subcategories."""
    labels_text = "\n".join([f"- {label}" for label in labels])
    prompt = f"""You are an expert biomedical data curator. Given the full set of labels for the main category "{category_name}", cluster them into at most {target_count} representative sub-categories.

REQUIREMENTS:
- Use concise, descriptive names that a human curator would recognize
- Assign every label to exactly one sub-category
- Avoid creating sub-categories with only 1-2 labels unless absolutely necessary
- Do NOT output an "Other" sub-category
- Output ONLY valid JSON, no explanations

LABELS:
{labels_text}

OUTPUT JSON FORMAT:
{{
  "subcategories": [
    {{"name": "Representative Name 1", "labels": ["..."]}} ,
    {{"name": "Representative Name 2", "labels": ["..."]}}
  ]
}}

JSON:"""
    return prompt


def llm_final_merge(category_name: str, labels: List[str], llm, sampling_params, target_count: int = 8) -> List[Dict]:
    """Use the LLM to produce the final consolidated subcategories (name + labels only), with retry."""
    prompt = create_final_merge_prompt(category_name, labels, target_count)
    try:
        outputs = llm.generate([prompt], sampling_params)
        response_text = outputs[0].outputs[0].text.strip()
        print(f"Final merge LLM response preview: {response_text[:200]}...")
        result = extract_json_from_response(response_text)
        if not (result and 'subcategories' in result):
            # Retry once with smaller target (to encourage grouping)
            prompt2 = create_final_merge_prompt(category_name, labels, max(4, target_count - 2))
            outputs = llm.generate([prompt2], sampling_params)
            response_text = outputs[0].outputs[0].text.strip()
            result = extract_json_from_response(response_text)
        if result and 'subcategories' in result:
            final_subcats = []
            input_set = set(labels)
            seen_labels = set()
            for subcat in result['subcategories']:
                if 'name' not in subcat or 'labels' not in subcat:
                    continue
                clean_labels = [l for l in subcat['labels'] if l in input_set and l not in seen_labels]
                if clean_labels:
                    seen_labels.update(clean_labels)
                    final_subcats.append({'name': subcat['name'], 'labels': clean_labels})
            # Reassign any missed labels to existing final subcategories
            missed = [l for l in labels if l not in seen_labels]
            if missed and final_subcats:
                mapping = llm_reassign_leftovers(category_name, final_subcats, missed, llm, sampling_params)
                remaining = [l for l in missed if l not in mapping]
                if remaining:
                    heuristic_map = heuristic_reassign_leftovers(final_subcats, remaining)
                    mapping.update(heuristic_map)
                name_to_idx = {sc['name']: i for i, sc in enumerate(final_subcats)}
                for label, subname in mapping.items():
                    idx = name_to_idx.get(subname)
                    if idx is not None:
                        final_subcats[idx]['labels'].append(label)
                        seen_labels.add(label)
            if not final_subcats:
                final_subcats = [{'name': 'Cluster 1', 'labels': labels}]
            return final_subcats
        # Fallback
        return [{'name': 'Cluster 1', 'labels': labels}]
    except Exception as e:
        print(f"✗ Error in final LLM merge: {e}")
        return [{'name': 'Cluster 1', 'labels': labels}]


def create_subcat_merge_prompt(category_name: str, subcats: List[Dict], target_count: int = 8) -> str:
    """Prompt to merge existing subcategories into up to target_count higher-level groups.
    Returns mapping from subcategory names to merged group names.
    """
    examples = []
    for sc in subcats:
        sample = sc['labels'][:4]
        examples.append(f"- {sc['name']}: {sample}")
    examples_text = "\n".join(examples)
    subcat_names = ", ".join([sc['name'] for sc in subcats])
    prompt = f"""
You are an expert biomedical data curator. Merge the existing sub-categories for the main category "{category_name}" into at most {target_count} coherent, human-readable groups.

REQUIREMENTS:
- Use concise, descriptive group names that do not include the main category name
- Assign EVERY sub-category to exactly ONE group
- Do NOT create an "Other" group
- Output ONLY valid JSON enclosed in a single ```json fenced block

EXISTING SUB-CATEGORIES:
{subcat_names}

EXAMPLES (few labels per sub-category to guide grouping):
{examples_text}

OUTPUT JSON FORMAT:
```json
{{
  "groups": [
    {{"name": "Group Name 1", "subcategories": ["subcat name a", "subcat name b"]}},
    {{"name": "Group Name 2", "subcategories": ["subcat name c"]}}
  ]
}}
```
"""
    return prompt


def extract_json_block(response_text: str) -> str:
    """Extract the content inside a ```json ... ``` block if present; else return raw."""
    m = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", response_text)
    if m:
        return m.group(1)
    return response_text


def llm_merge_subcategories(category_name: str, subcats: List[Dict], llm, sampling_params, target_count: int = 8) -> List[Dict]:
    """Merge existing subcategories into at most target_count groups using the LLM.
    Returns list of merged subcategories with aggregated labels.
    """
    if not subcats:
        return []
    prompt = create_subcat_merge_prompt(category_name, subcats, target_count)
    try:
        outputs = llm.generate([prompt], sampling_params)
        text = outputs[0].outputs[0].text.strip()
        json_str = extract_json_block(text)
        data = extract_json_from_response(json_str) or extract_json_from_response(text)
        merged = []
        if data and isinstance(data.get('groups'), list):
            name_to_labels: Dict[str, List[str]] = {}
            # Map subcat name -> labels
            subcat_lookup = {sc['name']: sc['labels'] for sc in subcats}
            for grp in data['groups']:
                gname = grp.get('name')
                sc_names = grp.get('subcategories', [])
                if not gname or not sc_names:
                    continue
                labels_accum: List[str] = []
                for scn in sc_names:
                    labels_accum.extend(subcat_lookup.get(scn, []))
                # Deduplicate labels
                seen = set()
                deduped = []
                for l in labels_accum:
                    if l not in seen:
                        seen.add(l)
                        deduped.append(l)
                if deduped:
                    merged.append({"name": gname, "labels": deduped})
        return merged
    except Exception as e:
        print(f"✗ Error in llm_merge_subcategories: {e}")
        return []


def heuristic_merge_subcategories(subcats: List[Dict], target_count: int = 8) -> List[Dict]:
    """Heuristic fallback: merge subcategories by name similarity until <= target_count groups."""
    if len(subcats) <= target_count:
        return subcats
    groups: List[List[Dict]] = []
    for sc in subcats:
        placed = False
        for g in groups:
            # Compare to first name in group
            ref = g[0]['name']
            score = SequenceMatcher(None, sc['name'].lower(), ref.lower()).ratio()
            if score >= 0.45:
                g.append(sc)
                placed = True
                break
        if not placed:
            groups.append([sc])
    # If still many groups, merge smallest by name similarity
    while len(groups) > target_count:
        groups.sort(key=lambda x: sum(len(s['labels']) for s in x))
        g1 = groups.pop(0)
        # Merge into most similar group
        best_idx, best_score = 0, -1.0
        for i, g in enumerate(groups):
            score = SequenceMatcher(None, g1[0]['name'].lower(), g[0]['name'].lower()).ratio()
            if score > best_score:
                best_score, best_idx = score, i
        groups[best_idx].extend(g1)
    # Build merged
    merged: List[Dict] = []
    for g in groups:
        all_labels: List[str] = []
        names: List[str] = []
        for sc in g:
            all_labels.extend(sc['labels'])
            names.append(sc['name'])
        seen = set()
        deduped = []
        for l in all_labels:
            if l not in seen:
                seen.add(l)
                deduped.append(l)
        merged_name = max(names, key=len)
        merged.append({"name": merged_name, "labels": deduped})
    return merged

def create_naming_prompt(category_name: str, subcats: List[Dict]) -> str:
    """Create a prompt to assign concise, descriptive names to each subcategory based on its labels."""
    entries = []
    for sc in subcats:
        sample = sc['labels'][:12]
        entries.append(f"- {sc['name']}: {sample}")
    entries_text = "\n".join(entries)
    prompt = f"""
You are an expert biomedical data curator. For the main category "{category_name}", assign a concise, descriptive name to EACH sub-category below based on its example labels.

REQUIREMENTS:
- Provide meaningful names that do NOT include the main category name
- Keep names short and human-readable
- Output ONLY valid JSON enclosed in a single ```json fenced block

SUB-CATEGORIES WITH EXAMPLE LABELS:
{entries_text}

OUTPUT JSON FORMAT:
```json
{{
  "names": [
    {{"old": "existing subcat name", "new": "Concise descriptive name"}}
  ]
}}
```
"""
    return prompt


def llm_name_subcategories(category_name: str, subcats: List[Dict], llm, sampling_params) -> List[Dict]:
    """Rename generic subcategory names using LLM suggestions; fallback keeps original."""
    if not subcats:
        return subcats
    prompt = create_naming_prompt(category_name, subcats)
    try:
        outputs = llm.generate([prompt], sampling_params)
        text = outputs[0].outputs[0].text.strip()
        json_str = extract_json_block(text)
        data = None
        try:
            data = json.loads(json_str)
        except Exception:
            try:
                data = json.loads(text[text.find('{'):text.rfind('}')+1])
            except Exception:
                data = None
        mapping: Dict[str, str] = {}
        if data and isinstance(data.get('names'), list):
            for item in data['names']:
                old = item.get('old')
                new = item.get('new')
                if old and new:
                    mapping[old] = new
        # Apply mapping to any generic names
        renamed: List[Dict] = []
        for sc in subcats:
            old = sc['name']
            if old in mapping:
                renamed.append({"name": mapping[old], "labels": sc['labels']})
            else:
                renamed.append(sc)
        return renamed
    except Exception as e:
        print(f"✗ Error in llm_name_subcategories: {e}")
        return subcats


def needs_naming(subcats: List[Dict]) -> bool:
    for sc in subcats:
        n = sc['name'].lower()
        if n.startswith('chunk') or n.startswith('cluster'):
            return True
    return False

# Apply naming after pipelines where appropriate
# 1) After hierarchical merge fallback
# (modify process_category_in_batches to call naming if generic names persist)

# Wire hierarchical merge into the batch pipeline

def base_process_category_in_batches(category_name: str, labels: List[str], 
                                    llm, sampling_params, batch_size: int = 100) -> List[Dict]:
    """Original batching + final-merge pipeline."""
    if len(labels) <= batch_size:
        # Small category - process as single batch directly to target_count
        result = llm_subclustering(category_name, labels, llm, sampling_params, max_subcategories=8)
        return result['subcategories'] if result else [{'name': 'Cluster 1', 'labels': labels}]
    
    print(f"Processing {category_name} in batches (total labels: {len(labels)})")
    
    all_subcategories = []
    batch_num = 0
    
    for i in range(0, len(labels), batch_size):
        batch = labels[i:i+batch_size]
        batch_num += 1
        
        print(f"  Processing batch {batch_num} ({len(batch)} labels)...")
        
        result = llm_subclustering(category_name, batch, llm, sampling_params, max_subcategories=6)
        
        if result and 'subcategories' in result:
            all_subcategories.extend(result['subcategories'])
        
        # Small delay between batches
        time.sleep(1)
    
    # Final LLM-based consolidation: use the full label list to produce <= target_count groups
    unique_labels = []
    seen = set()
    for subcat in all_subcategories:
        for l in subcat['labels']:
            if l not in seen:
                seen.add(l)
                unique_labels.append(l)
    
    final_subcategories = llm_final_merge(category_name, unique_labels, llm, sampling_params, target_count=8)
    return final_subcategories


def process_category_in_batches(category_name: str, labels: List[str], 
                               llm, sampling_params, batch_size: int = 100) -> List[Dict]:
    """Wrapper that uses the base pipeline and, if it collapses to one cluster, attempts hierarchical merging."""
    prelim = base_process_category_in_batches(category_name, labels, llm, sampling_params, batch_size)
    if not prelim:
        return []
    # If the result is a single generic cluster, attempt hierarchical merge using pseudo-subcategories
    if len(prelim) == 1 and prelim[0]['name'].lower().startswith('cluster'):
        print("Result collapsed to a single cluster; attempting hierarchical merge of subcategories...")
        # Create pseudo-subcategories by chunking labels deterministically
        pseudo_subcats: List[Dict] = []
        chunk = max(30, min(100, len(labels)//6 or 30))
        for i in range(0, len(labels), chunk):
            pseudo_subcats.append({"name": f"Chunk {i//chunk+1}", "labels": labels[i:i+chunk]})
        # Try LLM-based merge of pseudo subcats
        merged = llm_merge_subcategories(category_name, pseudo_subcats, llm, sampling_params, target_count=8)
        if merged:
            # Name the merged groups if needed
            if needs_naming(merged):
                merged = llm_name_subcategories(category_name, merged, llm, sampling_params)
            return merged
        # Heuristic fallback
        merged = heuristic_merge_subcategories(pseudo_subcats, target_count=8)
        if needs_naming(merged):
            merged = llm_name_subcategories(category_name, merged, llm, sampling_params)
        return merged
    # Name prelim if contains generic placeholders
    if needs_naming(prelim):
        prelim = llm_name_subcategories(category_name, prelim, llm, sampling_params)
    return prelim


# ------------------------- Saving / Summary -------------------------

def save_category_results(category_name: str, subcategories: List[Dict], 
                         output_file: str, append: bool = True):
    """Save results for a single category incrementally (name + labels only)."""
    
    print(f"Saving results for '{category_name}' to file {output_file}...")
    
    # Prepare data for this category
    output_data = []
    
    for subcat in subcategories:
        output_data.append({
            'main_category': category_name,
            'subcategory': subcat['name'],
            'labels': str(subcat['labels'])
        })
    
    # Create DataFrame
    output_df = pd.DataFrame(output_data)
    
    # Write to file (append mode if file exists)
    write_header = not (append and os.path.exists(output_file))
    output_df.to_csv(output_file, mode='a' if append else 'w', 
                     header=write_header, index=False)
    
    print(f"✓ Saved {len(subcategories)} subcategories for '{category_name}'")
    print(f"  - {len(output_data)} rows added to {output_file}")


# ------------------------- Main -------------------------

def print_category_summary(category_name: str, subcategories: List[Dict]):
    """Print summary for a single category."""
    
    total_labels = sum(len(subcat['labels']) for subcat in subcategories)
    
    print(f"\n--- {category_name} Summary ---")
    print(f"Sub-categories created: {len(subcategories)}")
    print(f"Total labels: {total_labels}")
    
    for i, subcat in enumerate(subcategories, 1):
        print(f"  {i}. {subcat['name']}: {len(subcat['labels'])} labels")


def main():
    """Main execution function."""
    print("=== LLM-based Sub-clustering within Categories ===")

    paths = load_paths()
    settings = load_settings()
    clustered_file = paths["sample_characteristics_clustered"]
    output_file = paths["sample_characteristics_subclustered_generated"]
    model_path = os.environ.get("RDAS_LLM_MODEL_PATH") or settings.get("llm_model_path")
    
    # Check input file
    if not os.path.exists(clustered_file):
        print(f"✗ Input file not found: {clustered_file}")
        return
    
    try:
        # Reset output file at start to avoid resume/skip behavior
        if os.path.exists(output_file):
            os.remove(output_file)
            print(f"Reset output file: {output_file}")
        
        # Load data
        print(f"\nLoading input data...")
        categories_data = load_clustered_data(clustered_file)
        
        # Initialize LLM
        if not VLLM_AVAILABLE:
            print("✗ vLLM not available")
            return

        if not model_path:
            print("✗ No LLM model path configured. Set RDAS_LLM_MODEL_PATH or settings.llm_model_path.")
            return

        llm, sampling_params = initialize_llm(model_path)
        if llm is None:
            print("✗ Failed to initialize LLM")
            return
        
        print(f"\n=== PROCESSING CATEGORIES ===")
        
        processed_count = 0
        skipped_count = 0
        target_categories = list(categories_data.keys())
        total_categories = len(target_categories)
        
        for idx, category_name in enumerate(target_categories, start=1):
            labels = categories_data.get(category_name, [])
            
            print(f"\n[{idx}/{total_categories}] Processing '{category_name}':")
            print(f"  Labels: {len(labels):,}")
            
            # Process even if small or empty
            if not labels:
                print("  No labels found for this category; writing empty result entry is skipped")
                skipped_count += 1
                continue
            
            # Process category
            subcategories = process_category_in_batches(
                category_name, labels, llm, sampling_params, batch_size=100
            )
            
            if not subcategories:
                # Fallback: single cluster
                subcategories = [{'name': 'Cluster 1', 'labels': labels}]
            
            # Save results immediately
            ensure_parent_dir(output_file)
            save_category_results(
                category_name, subcategories, output_file, 
                append=(processed_count > 0)
            )
            
            # Print category summary
            print_category_summary(category_name, subcategories)
            
            processed_count += 1
            print(f"✓ Completed {processed_count} categories, {skipped_count} skipped")
            
            # Small delay between categories
            time.sleep(1)
        
        # Final summary
        print(f"\n=== FINAL SUMMARY ===")
        print(f"Categories processed: {processed_count}")
        print(f"Categories skipped (no labels): {skipped_count}")
        print(f"Total target categories: {total_categories}")
        
        if processed_count > 0:
            print(f"\n✓ Sub-clustering completed successfully!")
            print(f"Results saved to:")
            print(f"  - {output_file}")
        else:
            print(f"\n⚠ No categories were processed")
        
    except Exception as e:
        print(f"✗ Error during sub-clustering: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 
