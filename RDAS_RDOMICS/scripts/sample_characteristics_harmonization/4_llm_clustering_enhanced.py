#!/usr/bin/env python3
"""
LLM-based Sample Characteristics Clustering with Confidence Scoring

This script clusters biomedical sample labels into predefined categories using:
1. Keyword-based pre-filtering for obvious matches
2. Similarity-based matching for related terms  
3. LLM-based categorization for complex cases
4. Confidence scoring for all categorizations

INPUT FILES:
-----------
1. 3_sample_characteristics_key_count_english_only.csv - Labels to be clustered
2. sample_characteristics_vocabulary_fixed.csv - Target categories with definitions

OUTPUT:
-------
clustered_sample_characteristics.csv - Final categorization results
clustering_confidence_report.csv - Detailed confidence analysis
"""

import pandas as pd
import json
import numpy as np
from typing import List, Dict, Set, Tuple
import os
import sys
from pathlib import Path
from collections import defaultdict
from difflib import SequenceMatcher
import re
import time

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


def initialize_llm(model_path: str):
    """Initialize the Llama3 model using vLLM."""
    try:
        print("Initializing Llama3 model...")
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


def calculate_similarity(text1: str, text2: str) -> float:
    """Calculate similarity between two text strings."""
    return SequenceMatcher(None, text1.lower(), text2.lower()).ratio()


def load_input_data(labels_file: str, vocab_file: str) -> Tuple[List[Tuple[str, int]], Dict[str, Dict]]:
    """Load labels and vocabulary data."""
    
    print(f"Loading labels from {labels_file}...")
    labels_df = pd.read_csv(labels_file)
    labels_with_counts = [(row['name'], row['count']) for _, row in labels_df.iterrows()]
    print(f"✓ Loaded {len(labels_with_counts)} labels")
    
    print(f"Loading vocabulary from {vocab_file}...")
    vocab_df = pd.read_csv(vocab_file)
    
    categories = {}
    for _, row in vocab_df.iterrows():
        category_name = row['standarized_term']
        categories[category_name] = {
            'synonyms': row['synonym'] if pd.notna(row['synonym']) else '',
            'examples': row['Example_entrys'] if pd.notna(row['Example_entrys']) else '',
            'definition': row['Definition'] if pd.notna(row['Definition']) else ''
        }
    
    print(f"✓ Loaded {len(categories)} categories")
    
    return labels_with_counts, categories


def keyword_based_categorization(labels_with_counts: List[Tuple[str, int]], 
                                categories: Dict[str, Dict]) -> Tuple[Dict[str, List[Dict]], List[Tuple[str, int]]]:
    """Pre-categorize labels using enhanced keyword matching."""
    
    print("Starting keyword-based categorization...")
    
    # Enhanced keyword mappings based on categories
    keyword_mappings = {
        'External_sample_id': {
            'exact_matches': ['patient_id', 'sample_id', 'donor_id', 'subject_id', 'patient id', 'sample id', 'donor id', 'subject id'],
            'keywords': ['patient', 'id', 'sample', 'donor', 'subject', 'barcode', 'participant', 'individual', 'controlid', 'patientid', 'subjectid', 'sampleid'],
            'patterns': [r'.*id$', r'.*_id$', r'patient.*', r'subject.*', r'donor.*']
        },
        'Biospecimen Organism': {
            'exact_matches': ['species', 'organism', 'strain'],
            'keywords': ['mouse', 'human', 'homo sapiens', 'mus musculus', 'host', 'background strain', 'mouse strain'],
            'patterns': [r'.*strain.*', r'.*organism.*']
        },
        'Biospecimen Type': {
            'exact_matches': ['tissue', 'cell', 'blood', 'serum', 'organ', 'cell type', 'celltype', 'tissue type'],
            'keywords': ['lymphocyte', 'monocyte', 'macrophage', 'stem cell', 'biopsy', 'specimen', 'sample type', 'fluid', 'brain tissue', 'liver tissue'],
            'patterns': [r'.*tissue.*', r'.*cell.*', r'.*blood.*']
        },
        'Biospecimen Age': {
            'exact_matches': ['age', 'day', 'week', 'month', 'year', 'time'],
            'keywords': ['developmental stage', 'passage', 'age in', 'donor age', 'patient age', 'time point', 'timepoint'],
            'patterns': [r'.*age.*', r'.*day.*', r'.*time.*', r'.*stage.*']
        },
        'Biospecimen Sex': {
            'exact_matches': ['sex', 'gender', 'male', 'female'],
            'keywords': ['donor sex', 'patient gender', 'cell sex', 'animal sex'],
            'patterns': [r'.*sex.*', r'.*gender.*']
        },
        'Biospecimen Race': {
            'exact_matches': ['race', 'ethnicity'],
            'keywords': ['caucasian', 'asian', 'hispanic', 'ethnicity', 'donor race', 'donor ethnicity'],
            'patterns': [r'.*race.*', r'.*ethnicity.*', r'.*ethnic.*']
        },
        'Biospecimen Disease Condition': {
            'exact_matches': ['disease', 'condition', 'diagnosis', 'cancer', 'tumor', 'infection'],
            'keywords': ['disease state', 'disease status', 'clinical diagnosis', 'pathology', 'tumor type', 'cancer status', 'clinical condition'],
            'patterns': [r'.*disease.*', r'.*tumor.*', r'.*cancer.*', r'.*diagnosis.*', r'.*condition.*']
        },
        'Treatment': {
            'exact_matches': ['treatment', 'drug', 'therapy', 'medication', 'group', 'control', 'placebo'],
            'keywords': ['drug treatment', 'therapy', 'intervention', 'compound', 'chemical', 'vaccine', 'antibody treatment', 'chemotherapy'],
            'patterns': [r'.*treatment.*', r'.*drug.*', r'.*therapy.*', r'.*group.*']
        },
        'Treatment Dosage Regimen': {
            'exact_matches': ['dose', 'dosage', 'concentration', 'duration'],
            'keywords': ['treatment dose', 'drug dose', 'treatment duration', 'protocol', 'regimen', 'drug concentration'],
            'patterns': [r'.*dose.*', r'.*dosage.*', r'.*concentration.*', r'.*duration.*']
        }
    }
    
    keyword_categorizations = defaultdict(list)
    remaining_labels = []
    
    for label, count in labels_with_counts:
        label_lower = label.lower().strip()
        best_category = None
        confidence = 'LOW'
        matched_term = None
        match_type = None
        
        # Check each category
        for category, mapping in keyword_mappings.items():
            
            # Check exact matches first (highest confidence)
            for exact_match in mapping['exact_matches']:
                if exact_match.lower() == label_lower or exact_match.lower() in label_lower.split():
                    best_category = category
                    confidence = 'HIGH'
                    matched_term = exact_match
                    match_type = 'exact_match'
                    break
            
            if best_category:
                break
            
            # Check keyword matches (medium confidence)
            for keyword in mapping['keywords']:
                if keyword.lower() in label_lower:
                    best_category = category
                    confidence = 'MEDIUM'
                    matched_term = keyword
                    match_type = 'keyword'
                    break
            
            if best_category:
                break
            
            # Check pattern matches (lower confidence)
            for pattern in mapping['patterns']:
                if re.search(pattern, label_lower):
                    if confidence != 'MEDIUM':  # Don't override higher confidence
                        best_category = category
                        confidence = 'MEDIUM' if 'id' in pattern or 'age' in pattern else 'LOW'
                        matched_term = pattern
                        match_type = 'pattern'
                    break
        
        if best_category:
            keyword_categorizations[best_category].append({
                'label': label,
                'count': count,
                'matched_term': matched_term,
                'match_type': match_type,
                'confidence': confidence
            })
        else:
            remaining_labels.append((label, count))
    
    total_processed = sum(len(results) for results in keyword_categorizations.values())
    print(f"✓ Keyword categorization completed:")
    print(f"  - {total_processed} labels categorized by keywords")
    print(f"  - {len(remaining_labels)} labels remaining")
    
    return dict(keyword_categorizations), remaining_labels


def similarity_based_categorization(remaining_labels: List[Tuple[str, int]], 
                                   keyword_results: Dict[str, List[Dict]], 
                                   threshold: float = 0.75) -> Tuple[Dict[str, List[Dict]], List[Tuple[str, int]]]:
    """Categorize remaining labels using similarity to already categorized labels."""
    
    print(f"Starting similarity-based categorization with threshold {threshold}...")
    
    # Build reference terms from keyword results
    reference_terms = {}
    for category, results in keyword_results.items():
        reference_terms[category] = [result['label'].lower() for result in results]
    
    similarity_categorizations = defaultdict(list)
    final_remaining = []
    
    for label, count in remaining_labels:
        best_match = None
        best_score = 0
        best_category = None
        
        for category, category_labels in reference_terms.items():
            for ref_label in category_labels:
                similarity = calculate_similarity(label, ref_label)
                
                if similarity > best_score and similarity >= threshold:
                    best_score = similarity
                    best_match = ref_label
                    best_category = category
        
        if best_match:
            similarity_categorizations[best_category].append({
                'label': label,
                'count': count,
                'similar_to': best_match,
                'similarity_score': best_score,
                'confidence': 'HIGH' if best_score >= 0.9 else 'MEDIUM'
            })
        else:
            final_remaining.append((label, count))
    
    total_processed = sum(len(results) for results in similarity_categorizations.values())
    print(f"✓ Similarity categorization completed:")
    print(f"  - {total_processed} labels categorized by similarity")
    print(f"  - {len(final_remaining)} labels remaining for LLM")
    
    return dict(similarity_categorizations), final_remaining


def create_clustering_prompt(labels_batch: List[Tuple[str, int]], 
                           categories: Dict[str, Dict]) -> str:
    """Create a clustering prompt for the LLM."""
    
    # Format category information
    category_info = "AVAILABLE CATEGORIES:\n"
    for i, (cat_name, cat_data) in enumerate(categories.items(), 1):
        category_info += f"{i}. {cat_name}\n"
        
        if cat_data.get('definition'):
            category_info += f"   Definition: {cat_data['definition'][:200]}...\n"
        
        if cat_data.get('synonyms'):
            synonyms = cat_data['synonyms'][:300]  # Limit length
            category_info += f"   Synonyms: {synonyms}...\n"
        
        category_info += "\n"
    
    # Format labels to categorize (show counts too for context)
    labels_text = ""
    for i, (label, count) in enumerate(labels_batch, 1):
        labels_text += f"{i}. \"{label}\" (count: {count})\n"
    
    prompt = f"""You are an expert biomedical data curator. Categorize these biomedical sample characteristic labels into the most appropriate predefined categories.

{category_info}

LABELS TO CATEGORIZE:
{labels_text}

INSTRUCTIONS:
- Assign each label to the MOST APPROPRIATE category above
- Consider the label's semantic meaning and biological context
- Use "Other" only if no category fits well
- Higher count labels are more important to categorize correctly

OUTPUT FORMAT - Respond with ONLY valid JSON:
{{
    "categorizations": [
        {{"label": "example_label", "category": "Biospecimen Type", "confidence": "HIGH", "reasoning": "Brief explanation"}},
        {{"label": "another_label", "category": "Treatment", "confidence": "MEDIUM", "reasoning": "Brief explanation"}}
    ]
}}

JSON:"""
    
    return prompt


def extract_json_from_llm_response(response_text: str) -> Dict:
    """Extract JSON from LLM response with error handling."""
    
    # Try to find JSON object
    if '{' in response_text and '}' in response_text:
        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}') + 1
        json_str = response_text[start_idx:end_idx]
        
        try:
            result = json.loads(json_str)
            if 'categorizations' in result and isinstance(result['categorizations'], list):
                return result
        except json.JSONDecodeError:
            pass
    
    # Look for JSON after "JSON:" marker
    if "JSON:" in response_text:
        json_part = response_text.split("JSON:")[-1].strip()
        if '{' in json_part and '}' in json_part:
            start_idx = json_part.find('{')
            end_idx = json_part.rfind('}') + 1
            json_str = json_part[start_idx:end_idx]
            
            try:
                result = json.loads(json_str)
                if 'categorizations' in result and isinstance(result['categorizations'], list):
                    return result
            except json.JSONDecodeError:
                pass
    
    return None


def llm_categorization(labels_batch: List[Tuple[str, int]], 
                      categories: Dict[str, Dict], 
                      llm, sampling_params, 
                      retry_count: int = 0) -> Dict:
    """LLM-based categorization with retry logic."""
    
    prompt = create_clustering_prompt(labels_batch, categories)
    
    try:
        outputs = llm.generate([prompt], sampling_params)
        response_text = outputs[0].outputs[0].text.strip()
        
        print(f"LLM Response preview: {response_text[:150]}...")
        
        result = extract_json_from_llm_response(response_text)
        
        if result:
            # Validate and clean results
            valid_categories = list(categories.keys())
            for cat_result in result['categorizations']:
                if 'confidence' not in cat_result:
                    cat_result['confidence'] = 'MEDIUM'
                if cat_result['category'] not in valid_categories:
                    cat_result['category'] = 'Other'
                    cat_result['confidence'] = 'LOW'
            
            print(f"✓ Successfully categorized {len(result['categorizations'])} labels")
            return result
        else:
            print("✗ No valid JSON found in LLM response")
            
            # Retry with smaller batch
            if retry_count == 0 and len(labels_batch) > 3:
                print(f"Retrying with smaller batch...")
                mid = len(labels_batch) // 2
                batch1 = labels_batch[:mid]
                batch2 = labels_batch[mid:]
                
                result1 = llm_categorization(batch1, categories, llm, sampling_params, retry_count + 1)
                result2 = llm_categorization(batch2, categories, llm, sampling_params, retry_count + 1)
                
                if result1 and result2:
                    combined_result = {
                        'categorizations': result1['categorizations'] + result2['categorizations']
                    }
                    return combined_result
            
            return None
            
    except Exception as e:
        print(f"✗ Error in LLM categorization: {e}")
        return None


def combine_all_results(keyword_results: Dict, similarity_results: Dict, 
                       llm_results: Dict, categories: Dict) -> Tuple[Dict, List]:
    """Combine results from all categorization methods."""
    
    final_results = {}
    confidence_report = []
    
    # Initialize categories
    for category in categories.keys():
        final_results[category] = []
    
    # Process keyword results
    for category, results in keyword_results.items():
        for result in results:
            final_results[category].append(result['label'])
            confidence_report.append({
                'label': result['label'],
                'count': result['count'],
                'category': category,
                'method': 'keyword',
                'confidence': result['confidence'],
                'details': f"{result['match_type']}: {result['matched_term']}"
            })
    
    # Process similarity results
    for category, results in similarity_results.items():
        for result in results:
            final_results[category].append(result['label'])
            confidence_report.append({
                'label': result['label'],
                'count': result['count'],
                'category': category,
                'method': 'similarity',
                'confidence': result['confidence'],
                'details': f"Similar to: {result['similar_to']} (score: {result['similarity_score']:.3f})"
            })
    
    # Process LLM results
    for category, results in llm_results.items():
        for result in results:
            final_results[category].append(result['label'])
            confidence_report.append({
                'label': result['label'],
                'count': result.get('count', 0),
                'category': category,
                'method': 'llm',
                'confidence': result.get('confidence', 'MEDIUM'),
                'details': result.get('reasoning', 'LLM categorization')
            })
    
    return final_results, confidence_report


def save_results(final_results: Dict, confidence_report: List, 
                output_file: str, confidence_file: str):
    """Save clustering results."""
    
    # Save final categorization results
    output_data = []
    for category, labels_list in final_results.items():
        # Calculate total count for this category
        total_count = 0
        for report in confidence_report:
            if report['category'] == category:
                total_count += report.get('count', 0)
        
        output_data.append({
            'standardized_term': category,
            'label_count': len(labels_list),
            'total_occurrence_count': total_count,
            'labels': str(labels_list)
        })
    
    output_df = pd.DataFrame(output_data)
    output_df = output_df.sort_values('total_occurrence_count', ascending=False)
    output_df.to_csv(output_file, index=False)
    
    # Save confidence report
    confidence_df = pd.DataFrame(confidence_report)
    confidence_df = confidence_df.sort_values(['category', 'count'], ascending=[True, False])
    confidence_df.to_csv(confidence_file, index=False)
    
    print(f"✓ Results saved to {output_file}")
    print(f"✓ Confidence report saved to {confidence_file}")


def print_summary(final_results: Dict, confidence_report: List):
    """Print clustering summary."""
    
    print("\n=== CLUSTERING SUMMARY ===")
    total_labels = sum(len(labels) for labels in final_results.values())
    total_occurrences = sum(report.get('count', 0) for report in confidence_report)
    
    print(f"Total labels clustered: {total_labels}")
    print(f"Total label occurrences: {total_occurrences:,}")
    
    # Category summary
    print(f"\nCATEGORY BREAKDOWN:")
    category_stats = []
    for category, labels_list in final_results.items():
        cat_count = sum(report.get('count', 0) for report in confidence_report 
                       if report['category'] == category)
        category_stats.append((category, len(labels_list), cat_count))
    
    category_stats.sort(key=lambda x: x[2], reverse=True)  # Sort by occurrence count
    
    for category, label_count, occurrence_count in category_stats:
        percentage = (occurrence_count / total_occurrences * 100) if total_occurrences > 0 else 0
        print(f"  {category}: {label_count} labels ({occurrence_count:,} occurrences, {percentage:.1f}%)")
    
    # Confidence analysis
    confidence_counts = {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
    method_counts = {'keyword': 0, 'similarity': 0, 'llm': 0}
    
    for report in confidence_report:
        confidence_counts[report['confidence']] += 1
        method_counts[report['method']] += 1
    
    print(f"\nCONFIDENCE ANALYSIS:")
    for confidence, count in confidence_counts.items():
        percentage = (count / total_labels * 100) if total_labels > 0 else 0
        print(f"  {confidence}: {count} labels ({percentage:.1f}%)")
    
    print(f"\nMETHOD ANALYSIS:")
    for method, count in method_counts.items():
        percentage = (count / total_labels * 100) if total_labels > 0 else 0
        print(f"  {method.title()}: {count} labels ({percentage:.1f}%)")


def main():
    """Main execution function."""
    print("=== LLM-based Sample Characteristics Clustering ===")

    paths = load_paths()
    settings = load_settings()
    labels_file = paths["sample_characteristics_key_count_english_only"]
    vocab_file = paths["sample_characteristics_vocabulary"]
    output_file = paths["sample_characteristics_clustered"]
    confidence_file = paths["sample_characteristics_confidence_report"]
    model_path = os.environ.get("RDAS_LLM_MODEL_PATH") or settings.get("llm_model_path")
    
    # Check input files
    if not os.path.exists(labels_file):
        print(f"✗ Labels file not found: {labels_file}")
        return
    
    if not os.path.exists(vocab_file):
        print(f"✗ Vocabulary file not found: {vocab_file}")
        return
    
    try:
        # Load data
        print(f"\nLoading input data...")
        labels_with_counts, categories = load_input_data(labels_file, vocab_file)
        
        # Stage 1: Keyword-based categorization
        print(f"\n=== STAGE 1: KEYWORD-BASED CATEGORIZATION ===")
        keyword_results, remaining_after_keywords = keyword_based_categorization(
            labels_with_counts, categories
        )
        
        # Stage 2: Similarity-based categorization
        print(f"\n=== STAGE 2: SIMILARITY-BASED CATEGORIZATION ===")
        similarity_results, remaining_after_similarity = similarity_based_categorization(
            remaining_after_keywords, keyword_results, threshold=0.75
        )
        
        # Stage 3: LLM-based categorization
        print(f"\n=== STAGE 3: LLM-BASED CATEGORIZATION ===")
        llm_results = defaultdict(list)
        
        if remaining_after_similarity:
            print(f"Processing {len(remaining_after_similarity)} labels with LLM...")
            
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
            
            # Sort by count (descending) for better LLM performance on important labels
            remaining_after_similarity.sort(key=lambda x: x[1], reverse=True)
            
            # Process in small batches
            batch_size = 8
            
            for i in range(0, len(remaining_after_similarity), batch_size):
                batch = remaining_after_similarity[i:i+batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(remaining_after_similarity) + batch_size - 1) // batch_size
                
                print(f"Processing LLM batch {batch_num}/{total_batches} ({len(batch)} labels)...")
                
                llm_result = llm_categorization(batch, categories, llm, sampling_params)
                
                if llm_result and 'categorizations' in llm_result:
                    for cat_result in llm_result['categorizations']:
                        # Find original count
                        original_count = next((count for label, count in batch 
                                             if label == cat_result['label']), 0)
                        cat_result['count'] = original_count
                        
                        category = cat_result['category']
                        llm_results[category].append(cat_result)
                    
                    print(f"✓ Batch {batch_num} processed successfully")
                else:
                    print(f"✗ Batch {batch_num} failed, adding to Other")
                    for label, count in batch:
                        llm_results['Other'].append({
                            'label': label,
                            'count': count,
                            'confidence': 'LOW',
                            'reasoning': 'LLM processing failed'
                        })
                
                # Small delay between batches
                time.sleep(1)
        else:
            print("✓ No labels remaining for LLM processing")
        
        # Combine all results
        print(f"\n=== COMBINING RESULTS ===")
        final_results, confidence_report = combine_all_results(
            keyword_results, similarity_results, llm_results, categories
        )
        
        # Save results
        ensure_parent_dir(output_file)
        ensure_parent_dir(confidence_file)
        save_results(final_results, confidence_report, output_file, confidence_file)
        
        # Print summary
        print_summary(final_results, confidence_report)
        
        print(f"\n✓ Clustering completed successfully!")
        
    except Exception as e:
        print(f"✗ Error during clustering: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 
