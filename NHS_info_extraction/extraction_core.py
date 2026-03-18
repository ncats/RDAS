import json
import time
import re
import requests
from vllm import LLM, SamplingParams
from typing import Dict, List, Optional


class TerminologyEnhancer:
    """Simplified enhancer for HPO and RxNorm IDs"""
    
    def __init__(self, enable_api_calls=True, timeout=10, verbose=True, proxy_url=None):
        self.hpo_api = "https://clinicaltables.nlm.nih.gov/api/hpo/v3/search"
        self.rxnorm_api = "https://rxnav.nlm.nih.gov/REST"
        self.hpo_cache = {}
        self.rxnorm_cache = {}
        self.timeout = timeout
        self.verbose = verbose
        self.enabled = enable_api_calls
        
        # Proxy configuration
        self.proxies = None
        if proxy_url:
            self.proxies = {'http': proxy_url, 'https': proxy_url}
            if verbose:
                print(f"Using proxy: {proxy_url}")
        
        # Simple statistics
        self.stats = {'hpo_matched': 0, 'hpo_not_found': 0, 
                     'rxnorm_matched': 0, 'rxnorm_not_found': 0,
                     'api_calls': 0, 'cache_hits': 0}
    
    def _api_call(self, url, params, cache_key=None, cache_dict=None):
        """Unified API call handler with caching"""
        if not self.enabled:
            return None
        
        # Check cache
        if cache_key and cache_dict and cache_key in cache_dict:
            self.stats['cache_hits'] += 1
            return cache_dict[cache_key]
        
        # Make API call
        try:
            self.stats['api_calls'] += 1
            response = requests.get(url, params=params, timeout=self.timeout, proxies=self.proxies)
            if response.status_code == 200:
                result = response.json()
                if cache_key and cache_dict is not None:
                    cache_dict[cache_key] = result
                return result
        except requests.exceptions.RequestException as e:
            if self.verbose and self.stats['api_calls'] <= 5:
                print(f"API error: {str(e)[:50]}...")
            # Disable after too many failures
            if self.stats['api_calls'] > 10 and self.stats['cache_hits'] == 0:
                self.enabled = False
                if self.verbose:
                    print("Disabling API calls due to repeated failures")
        return None
    
    def search_hpo(self, term: str) -> Optional[str]:
        """Search HPO and return best match ID"""
        data = self._api_call(
            self.hpo_api,
            {'terms': term, 'maxList': 1},
            cache_key=term,
            cache_dict=self.hpo_cache
        )
        
        if data and len(data) >= 4 and data[1] and data[3]:
            return data[1][0]  # First HPO code
        return None
    
    def search_rxnorm(self, term: str) -> Optional[str]:
        """Search RxNorm and return best match RXCUI"""
        # Try approximate search first
        data = self._api_call(
            f"{self.rxnorm_api}/approximateTerm.json",
            {'term': term, 'maxEntries': 1},
            cache_key=term,
            cache_dict=self.rxnorm_cache
        )
        
        if data:
            candidates = data.get('approximateGroup', {}).get('candidate', [])
            if candidates:
                rxcui = candidates[0].get('rxcui') if isinstance(candidates, list) else candidates.get('rxcui')
                if rxcui:
                    return rxcui
        
        # Fallback to direct name search
        data = self._api_call(
            f"{self.rxnorm_api}/rxcui.json",
            {'name': term}
        )
        
        if data:
            rxnorm_id = data.get('idGroup', {}).get('rxnormId')
            if rxnorm_id:
                return rxnorm_id[0] if isinstance(rxnorm_id, list) else rxnorm_id
        
        return None
    
    def enhance_outcomes(self, outcomes_str: str) -> str:
        """Add HPO IDs to clinical outcomes"""
        if not outcomes_str or outcomes_str == "N/A":
            return outcomes_str
        
        outcomes = [o.strip() for o in outcomes_str.split(',')]
        enhanced = []
        
        for outcome in outcomes:
            # Remove existing HPO IDs
            clean = re.sub(r'\s*\[(HP|HPO):[^\]]+\]', '', outcome).strip()
            if not clean:
                continue
            
            hpo_id = self.search_hpo(clean)
            if hpo_id:
                enhanced.append(f"{clean} [{hpo_id}]")
                self.stats['hpo_matched'] += 1
                if self.verbose:
                    print(f"HPO: '{clean}' -> {hpo_id}")
            else:
                enhanced.append(clean)
                self.stats['hpo_not_found'] += 1
            
            time.sleep(0.2)
        
        return ', '.join(enhanced)
    
    def enhance_treatments(self, treatments_str: str) -> str:
        """Add RxNorm IDs to treatments"""
        if not treatments_str or treatments_str == "N/A":
            return treatments_str
        
        treatments = [t.strip() for t in treatments_str.split(',')]
        enhanced = []
        
        # Non-pharmacologic keywords to skip
        skip_keywords = ['transplant', 'surgery', 'therapy', 'ventilation',
                        'dialysis', 'transfusion', 'resection', 'radiation']
        
        for treatment in treatments:
            # Remove existing RxNorm IDs
            clean = re.sub(r'\s*\[RxNorm:[^\]]+\]', '', treatment).strip()
            
            # Extract drug name (before parentheses)
            drug_match = re.match(r'^([^(]+)', clean)
            if not drug_match:
                enhanced.append(clean)
                continue
            
            drug_name = drug_match.group(1).strip()
            
            # Skip non-pharmacologic interventions
            if any(kw in drug_name.lower() for kw in skip_keywords):
                enhanced.append(clean)
                continue
            
            if not self.enabled:
                enhanced.append(clean)
                continue
            
            rxcui = self.search_rxnorm(drug_name)
            if rxcui:
                enhanced.append(f"{clean} [RxNorm:{rxcui}]")
                self.stats['rxnorm_matched'] += 1
                if self.verbose:
                    print(f"RxNorm: '{drug_name}' -> {rxcui}")
            else:
                enhanced.append(clean)
                self.stats['rxnorm_not_found'] += 1
            
            time.sleep(0.2)
        
        return ', '.join(enhanced)
    
    def enhance_result(self, result: Dict) -> Dict:
        """Enhance a single extraction result with IDs"""
        if result.get("parse_error", False):
            return result
        
        enhanced = result.copy()
        
        if enhanced.get("clinical_outcomes"):
            enhanced["clinical_outcomes"] = self.enhance_outcomes(
                enhanced["clinical_outcomes"]
            )
        
        if enhanced.get("treatments_received"):
            enhanced["treatments_received"] = self.enhance_treatments(
                enhanced["treatments_received"]
            )
        
        return enhanced
    
    def print_stats(self):
        """Print summary statistics"""
        print("\n" + "="*60)
        print("API ENHANCEMENT SUMMARY")
        print("="*60)
        
        if not self.enabled:
            print("API enhancement was disabled (network issues or not enabled)")
            return
        
        hpo_total = self.stats['hpo_matched'] + self.stats['hpo_not_found']
        rx_total = self.stats['rxnorm_matched'] + self.stats['rxnorm_not_found']
        
        print(f"\nHPO: {self.stats['hpo_matched']}/{hpo_total} matched "
              f"({100*self.stats['hpo_matched']/hpo_total if hpo_total else 0:.1f}%)")
        print(f"RxNorm: {self.stats['rxnorm_matched']}/{rx_total} matched "
              f"({100*self.stats['rxnorm_matched']/rx_total if rx_total else 0:.1f}%)")
        print(f"\nAPI calls: {self.stats['api_calls']}, Cache hits: {self.stats['cache_hits']}")
        print("="*60)


def create_extraction_prompt(abstract: str) -> str:
    """Create the extraction prompt for a given abstract"""
    return f"""Extract the following characteristics from this natural history study abstract. 
If information is not available, use "N/A". 

Abstract: {abstract}

If an abstract states that a study was ongoing at the time of publishing or if there is no explicit end date mentioned, 
then please state "Started in [beginning date]" in the duration.

When identifying each field:
- Keep crucial details within each field while being as concise as possible.
- "disease_name": Full name of the disease or condition being studied.
- "study_purpose": Explicit aim of the study.
    - Example: "Assess the clinical characteristics of rare genetic disorder XYZ."
- "study_type": Type or design of the study (e.g., retrospective cohort, prospective registry, case series, etc.).
- "participants_count": Number of participants or records included in the study.
    - If sample size is 1, describe the individual participant as indicated.
- "data_collection_period": Time frame when data collection occurred (e.g., "2010–2018", "Started in 2015").
- "inclusion_criteria": Characteristics required for study inclusion.
- "exclusion_criteria": Characteristics leading to exclusion from the study.

- "clinical_outcomes": Comma-separated list of phenotypes observed in disease natural history:
signs/symptoms, disease manifestations, progression patterns, organ involvement, 
biomarker/functional score trajectories, natural complications (e.g., bleeding, cirrhosis), 
survival/clinical events reflecting disease biology (e.g., transplant, mortality).
Use short phrases (e.g., "progressive fibrosis", "elevated GGT", "declining lung function"). 
NO treatment efficacy wording. DO NOT include HPO IDs - just list the phenotypes.
Examples: "hepatomegaly", "elevated alkaline phosphatase", "progressive fibrosis", "declining FEV1".

- "treatments_received": Comma-separated list of treatments the population actually received.
Format: "treatment name/class"
Optionally note if initial vs maintenance/long-term therapy.
Examples: "LMWH", "enoxaparin", "second-line VEGF TKI", "gene therapy", 
"ursodeoxycholic acid off-label", "liver transplant".
NO comparative language ("more than", "better than"). NO efficacy claims—document only what was given.

- "study_duration": Overall duration of the study or follow-up period.
- "results": Summary of main findings or conclusions, keeping relevance to disease, purpose, and outcomes.

Provide your answer ONLY as valid JSON with these exact keys:
{{
  "disease_name": "...",
  "study_purpose": "...",
  "study_type": "...",
  "participants_count": "...",
  "data_collection_period": "...",
  "inclusion_criteria": "...",
  "exclusion_criteria": "...",
  "clinical_outcomes": "...",
  "treatments_received": "...",
  "study_duration": "...",
  "results": "..."
}}

Remember, output ONLY the JSON, nothing else. If your output results in any text outside of the JSON brackets "{{" or "}}", please remove them immediately.
"""


def extract_json_from_text(text: str) -> Dict:
    """Extract and parse JSON from model response text"""
    try:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start != -1 and end > start:
            json_str = text[start:end]
            return json.loads(json_str)
    except json.JSONDecodeError:
        pass
    return {"parse_error": True, "raw_response": text}


def process_abstracts(llm: LLM, abstracts: List[str], sampling_params: SamplingParams, batch_size: int = 10) -> List[Dict]:
    """Generate and parse model outputs in batches"""
    results = []
    total = len(abstracts)
    for i in range(0, total, batch_size):
        batch = abstracts[i : i + batch_size]
        prompts = [create_extraction_prompt(abstract) for abstract in batch]

        outputs = llm.generate(prompts, sampling_params)
        for output in outputs:
            response_text = output.outputs[0].text.strip() if output.outputs else ""
            result = extract_json_from_text(response_text)
            results.append(result)

        print(f"Processed {i + len(batch)}/{total} abstracts")
    return results