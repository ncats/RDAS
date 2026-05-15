"""
Author Disambiguation System
Disambiguates authors based on ORCID, affiliations, coauthors, and name patterns.
"""
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import ast
import time
import pandas as pd
from collections import deque
from typing import List, Dict
from itertools import combinations
from collections import Counter, defaultdict
from utils.applogger import AppLogger
from utils.tools import _elapsed_time, _date_string

class PersonDisambiguator:
    """
    A class to perform author disambiguation on publication/grant/clinical trial data.
    """
    
    def __init__(self, last_name, data_list: List[Dict], logger=None):
        """
        Initialize the disambiguator with data.
        
        Args:
            last_name (str): Last name currently being disambiguated.
            data_list (List[Dict]): List of dictionaries with author data
                Expected fields: ['id', 'associate_id', 'associate_type', 'source', 
                                 'first_name', 'last_name', 'affiliation', 'orcid', 
                                 'email', 'phone', 'first_publication_date', 'title', 
                                 'abstract_text', 'author_list', 'first_author', 'last_author']
            logger: Optional logger supplied by the caller. If omitted, the class
                creates its own file/console logger.
        """
        self.logger = logger or self.create_logger()
        self.df = None
        self.last_name = last_name
        self.load_from_dict_list(data_list)
        

    def create_logger(self):
        """Create a default logger for standalone person disambiguation runs."""

        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        class_name = type(self).__name__
        log_file = f"{log_dir}/F-{class_name}-{_date_string()}.log"

        return AppLogger(class_name, log_file).get_logger()
        

    def load_from_dict_list(self, data_list: List[Dict]):
        """
        Load data from a list of dictionaries.
        
        Args:
            data_list (List[Dict]): List of dictionaries with author data
        """
        # Define expected columns
        expected_columns = [
            'id', 'associate_id', 'associate_type', 'source',
            'first_name', 'last_name', 'affiliation', 'orcid',
            'email', 'phone', 'first_publication_date', 'title',
            'abstract_text', 'author_list', 'first_author', 'last_author'
        ]
        
        # Create DataFrame from list of dictionaries
        self.df = pd.DataFrame(data_list)
        
        # Ensure all expected columns exist
        for col in expected_columns:
            if col not in self.df.columns:
                self.df[col] = None
        
        # Convert author_list to proper format if it's a string
        if 'author_list' in self.df.columns:
            self.df['author_list'] = self.df['author_list'].apply(
                lambda x: x if isinstance(x, str) else (', '.join(x) if isinstance(x, list) else '')
            )
        
        self.logger.info(f"Last name: {self.last_name}, loaded {len(self.df)} records from dictionary list")

        # Auto-detect last_name from the data. Get the most common last name in the dataset
        """
        if 'last_name' in self.df.columns:
            last_name_counts = self.df['last_name'].value_counts()
            if len(last_name_counts) > 0:
                self.last_name = last_name_counts.index[0]
            else:
                self.last_name = "Unknown"
        else:
            self.last_name = "Unknown"       
        
        self.logger.info(f"Auto-detected last name: {self.last_name}")
        """


    def generate_name_set(self, first_name):
        """
        Generate variations of first name including initials.
        
        Args:
            first_name (str): First name to process
            
        Returns:
            list: Sorted list of name variations
        """
        if pd.isna(first_name) or not isinstance(first_name, str) or not first_name.strip():
            return []
        
        name_set = set()
        clean_name = first_name.strip()
        name_set.add(clean_name)
        name_set.add(clean_name.replace('-', ' '))
        
        parts = clean_name.replace('-', ' ').split()
        
        if parts:
            initials = ' '.join([p[0] for p in parts if p])
            if initials:
                name_set.add(initials)
            name_set.add(parts[0][0])
        
        return sorted(name_set)
    
    def get_first_initial(self, name_set):
        """Extract first initial from name_set."""
        if not name_set:
            return None
        for name_variant in name_set:
            if name_variant and isinstance(name_variant, str):
                return name_variant[0].upper()
        return None
    
    def get_two_initials(self, name_set):
        """Extract two initials from name_set."""
        if not name_set:
            return None
        
        for name_variant in name_set:
            if name_variant and isinstance(name_variant, str):
                parts = name_variant.strip().split()
                if len(parts) >= 2:
                    initials = ' '.join([p[0].upper() for p in parts if p])
                    if len(initials.replace(' ', '')) == 2:
                        return initials
        return None
    
    def process_initial_grouping(self):
        """
        Process initial grouping based on ORCID, affiliations, and coauthors.
        Creates: orcid_group, affil_group, coauthor_group columns.
        """
        # Parse and sort author lists
        self.df['author_list'] = self.df['author_list'].apply(
            lambda x: sorted([name.strip() for name in x.split(',')]) 
            if isinstance(x, str) and not pd.isna(x) 
            else (sorted(x) if isinstance(x, list) else [])
        )
        
        # Generate name variations
        self.df['name_set'] = self.df['first_name'].apply(self.generate_name_set)
        
        # ORCID grouping
        orcid_to_group = {orcid: f'group_{i+1}' 
                          for i, orcid in enumerate(self.df['orcid'].unique())}
        self.df['orcid_group'] = self.df['orcid'].map(orcid_to_group)
        
        max_group = self.df['orcid_group'].nunique()
        
        # Generate initials
        self.df['first_initial'] = self.df['name_set'].apply(self.get_first_initial)
        self.df['two_initials'] = self.df['name_set'].apply(self.get_two_initials)
        
        # Clean ORCID groups by majority initials
        self._clean_orcid_by_initials('first_initial')
        self._clean_orcid_by_initials('two_initials')
        
        # Affiliation grouping
        self._create_affiliation_groups(max_group)
        
        # Coauthor grouping
        self._create_coauthor_groups(max_group)
        
        # Clear baseline group
        self.df['orcid_group'] = self.df['orcid_group'].replace('group_1', '')
    
    def _clean_orcid_by_initials(self, initial_col):
        """Remove ORCID assignments that don't match majority initials."""
        for group, subdf in self.df.groupby('orcid_group'):
            if group == "group_1":
                continue
            
            initials = subdf[initial_col].dropna().tolist()
            unique_initials = set(initials)
            
            if not initials or len(unique_initials) <= 1:
                continue
            
            counter = Counter(initials)
            majority_initial, _ = counter.most_common(1)[0]
            
            mask = (self.df['orcid_group'] == group) & (self.df[initial_col] != majority_initial)
            self.df.loc[mask, 'orcid_group'] = ""
    
    def _create_affiliation_groups(self, max_group):
        """Create groups based on name + affiliation."""
        self.df['name_affil'] = list(zip(self.df['first_name'], 
                                         self.df['last_name'], 
                                         self.df['affiliation']))
        
        valid_name_affil = self.df[
            ~self.df['affiliation'].isna() & 
            (self.df['affiliation'].str.strip() != '')
        ]['name_affil']
        
        name_affil_counts = valid_name_affil.value_counts()
        repeated_affils = [k for k, v in name_affil_counts.items() if v >= 2]
        
        name_affil_to_group = {
            name_affil: f'group_{i + max_group + 1}'
            for i, name_affil in enumerate(repeated_affils)
        }
        
        self.df['affil_group'] = self.df['name_affil'].map(name_affil_to_group)
        self.df.drop(columns=['name_affil'], inplace=True)
    
    def _create_coauthor_groups(self, max_group):
        """Create groups based on name + coauthors."""
        self.df['coauthor_set'] = self.df['author_list'].apply(
            lambda x: tuple(sorted(set(x))) if isinstance(x, list) and len(set(x)) > 1 else None
        )
        
        self.df['name_coauthors_key'] = self.df.apply(
            lambda row: (row['first_name'], row['last_name'], row['coauthor_set']) 
            if row['coauthor_set'] else None,
            axis=1
        )
        
        valid_keys = self.df['name_coauthors_key'].dropna()
        key_counts = valid_keys.value_counts()
        repeated_keys = [k for k, v in key_counts.items() if v >= 2]
        
        starting_group = max_group + len(self.df['affil_group'].dropna().unique())
        coauthor_key_to_group = {
            key: f'group_{i + starting_group + 1}'
            for i, key in enumerate(repeated_keys)
        }
        
        self.df['coauthor_group'] = self.df['name_coauthors_key'].map(coauthor_key_to_group)
        self.df.drop(columns=['coauthor_set', 'name_coauthors_key'], inplace=True)
    
    def resolve_conflicts(self):
        """Resolve conflicts between ORCID and other grouping methods."""
        valid_rows_affil = self.df[
            self.df['orcid_group'].notna() & (self.df['orcid_group'] != '') &
            self.df['affil_group'].notna() & (self.df['affil_group'] != '')
        ]
        
        valid_rows_coauth = self.df[
            self.df['orcid_group'].notna() & (self.df['orcid_group'] != '') &
            self.df['coauthor_group'].notna() & (self.df['coauthor_group'] != '')
        ]
        
        affil_to_orcid_groups = valid_rows_affil.groupby('affil_group')['orcid_group'].nunique()
        coauth_to_orcid_groups = valid_rows_coauth.groupby('coauthor_group')['orcid_group'].nunique()
        
        shared_affils = affil_to_orcid_groups[affil_to_orcid_groups > 1].index.tolist()
        shared_coauths = coauth_to_orcid_groups[coauth_to_orcid_groups > 1].index.tolist()
        
        conflict_df_affils = valid_rows_affil[valid_rows_affil['affil_group'].isin(shared_affils)]
        conflict_df_coauth = valid_rows_coauth[valid_rows_coauth['coauthor_group'].isin(shared_coauths)]
        
        orcid_group_counts = self.df['orcid_group'].value_counts()
        unique_orcid_groups = orcid_group_counts[orcid_group_counts == 1].index.tolist()
        
        unique_conflict_affil = conflict_df_affils[conflict_df_affils['orcid_group'].isin(unique_orcid_groups)]
        unique_conflict_coauth = conflict_df_coauth[conflict_df_coauth['orcid_group'].isin(unique_orcid_groups)]
        
        self.df.loc[self.df['orcid_group'].isin(unique_conflict_affil['orcid_group']), 'orcid_group'] = ''
        self.df.loc[self.df['orcid_group'].isin(unique_conflict_coauth['orcid_group']), 'orcid_group'] = ''
    
    def create_merged_groups(self):
        """Merge ORCID, affiliation, and coauthor groups into unified groups."""
        self.df['merge_group'] = self.df['orcid_group'].fillna('')
        
        # Process affiliations
        self._merge_affiliation_groups()
        
        # Process coauthors
        self._merge_coauthor_groups()
    
    def _merge_affiliation_groups(self):
        """Merge affiliation groups with ORCID groups."""
        valid_affil = self.df[(self.df['affil_group'].notna()) & (self.df['affil_group'] != '')]
        affil_with_orcid = valid_affil[valid_affil['orcid_group'].notna() & (valid_affil['orcid_group'] != '')]
        
        affil_to_orcid = affil_with_orcid.groupby('affil_group')['orcid_group'].apply(list).to_dict()
        affil_groups_with_no_orcid = self.df[(self.df['orcid_group'].isna()) | 
                                          (self.df['orcid_group'] == '')]['affil_group'].dropna().unique()
        
        refined_affil_to_orcid = {}
        
        for affil in affil_groups_with_no_orcid:
            affil_rows = self.df[self.df['affil_group'] == affil]
            name_sets = affil_rows['name_set'].dropna().tolist()
            
            if len(name_sets) < 2:
                continue
            
            overlap = len(set.intersection(*map(set, name_sets)))
            
            if overlap >= 3:
                unique_orcid_groups = affil_rows['orcid_group'].dropna().unique()
                unique_orcid_groups = [og for og in unique_orcid_groups if og != '']
                
                if len(unique_orcid_groups) == 1:
                    merge_group_name = unique_orcid_groups[0]
                elif len(unique_orcid_groups) == 0:
                    merge_group_name = f"{affil}"
                else:
                    continue
                
                self.df.loc[self.df['affil_group'] == affil, 'merge_group'] = merge_group_name
        
        affil_without_orcid = set(valid_affil['affil_group'].unique()) - set(affil_to_orcid.keys())
        affil_mask_without_orcid = self.df['affil_group'].isin(affil_without_orcid)
        self.df.loc[affil_mask_without_orcid, 'merge_group'] = self.df.loc[affil_mask_without_orcid, 'affil_group']
    


    def _merge_coauthor_groups(self):

        """Merge coauthor groups with existing merged groups."""
        valid_coauth = self.df[(self.df['coauthor_group'].notna()) & (self.df['coauthor_group'] != '')]
        coauth_with_merge = valid_coauth[valid_coauth['merge_group'].notna() & 
                                         (valid_coauth['merge_group'] != '')]
        
        coauth_to_orcid = coauth_with_merge.groupby('coauthor_group')['merge_group'].apply(list).to_dict()
        coauth_groups_with_no_orcid = self.df[(self.df['orcid_group'].isna()) | 
                                           (self.df['orcid_group'] == '')]['coauthor_group'].dropna().unique()
        
        for coauth in coauth_groups_with_no_orcid:
            coauth_rows = self.df[self.df['coauthor_group'] == coauth]
            name_sets = coauth_rows['name_set'].dropna().tolist()
            
            if len(name_sets) < 2:
                continue
            
            overlap = len(set.intersection(*map(set, name_sets)))
            
            if overlap >= 3:
                unique_orcid_groups = coauth_rows['orcid_group'].dropna().unique()
                unique_orcid_groups = [og for og in unique_orcid_groups if og != '']
                
                if len(unique_orcid_groups) == 1:
                    merge_group_name = unique_orcid_groups[0]
                elif len(unique_orcid_groups) == 0:
                    merge_group_name = f"{coauth}"
                else:
                    continue
                
                self.df.loc[self.df['coauthor_group'] == coauth, 'merge_group'] = merge_group_name
        
        coauth_without_orcid = set(valid_coauth['coauthor_group'].unique()) - set(coauth_to_orcid.keys())
        coauth_mask_without_orcid = self.df['coauthor_group'].isin(coauth_without_orcid)
        self.df.loc[coauth_mask_without_orcid, 'merge_group'] = self.df.loc[coauth_mask_without_orcid, 'coauthor_group']
    


    def refine_with_coauthors(self):
        """Refine groups using high-frequency coauthor analysis."""
        self.df['author_list'] = self.df['author_list'].apply(
            lambda x: sorted([name.strip() for name in x.split(',')]) 
            if isinstance(x, str) and not pd.isna(x) 
            else (sorted(x) if isinstance(x, list) else [])
        )
        
        self.df['set_coauthor_group'] = self.df['merge_group'].fillna('')
        
        high_freq_by_group = self._calculate_high_freq_authors('merge_group')
        
        # Find connected components
        shared_matches = self._find_shared_coauthor_groups(high_freq_by_group)
        connected_components = self._build_connected_components(shared_matches)
        
        # Create mapping
        group_to_smallest = {}
        for component in connected_components:
            smallest = min(component)
            for group in component:
                group_to_smallest[group] = smallest
        
        self.df['set_coauthor_group'] = self.df['merge_group'].apply(
            lambda g: group_to_smallest.get(g, g)
        )
    


    def _calculate_high_freq_authors(self, group_col):
        """Calculate high-frequency authors for each group."""
        high_freq_by_group = {}
        grouped = self.df.groupby(group_col)
        
        for group_name, group_df in grouped:
            if group_name == "":
                continue
            
            author_counter = Counter()
            for authors in group_df['author_list']:
                author_counter.update(authors)
            
            name_set = set().union(*[set(names) for names in group_df['name_set']])
            group_size = len(group_df)
            threshold = group_size / 2
            
            high_freq_authors = [author for author, count in author_counter.items() 
                               if count >= threshold]
            
            high_freq_by_group[group_name] = {
                "size": group_size,
                "high_freq_authors": set(high_freq_authors),
                "name_set": set(name_set),
                "author_counts": dict(author_counter)
            }
        
        return high_freq_by_group
    

    
    def _find_shared_coauthor_groups(self, high_freq_by_group):
        """Find groups sharing high-frequency coauthors."""
        shared_matches = []
        
        for (group1, info1), (group2, info2) in combinations(high_freq_by_group.items(), 2):
            authors1 = info1["high_freq_authors"]
            authors2 = info2["high_freq_authors"]
            shared_authors = authors1.intersection(authors2)
            
            name_set1 = info1["name_set"]
            name_set2 = info2["name_set"]
            shared_names = name_set1.intersection(name_set2)
            
            cond1 = len(shared_authors) >= 4
            cond2 = any(author.startswith(self.last_name + " ") for author in shared_authors)
            cond3 = len(shared_names) >= 3
            
            if cond1 and cond2 and cond3:
                shared_matches.append((group1, group2, shared_authors, shared_names))
        
        return shared_matches
    

    
    def _build_connected_components(self, shared_matches):
        """Build connected components from shared matches."""
        graph = defaultdict(set)
        
        for g1, g2, _, _ in shared_matches:
            graph[g1].add(g2)
            graph[g2].add(g1)
        
        def dfs(node, visited, component):
            visited.add(node)
            component.add(node)
            for neighbor in graph[node]:
                if neighbor not in visited:
                    dfs(neighbor, visited, component)
        
        visited = set()
        connected_components = []
        
        for node in graph:
            if node not in visited:
                component = set()
                dfs(node, visited, component)
                connected_components.append(component)
        
        return connected_components
    

    
    def assign_unified_groups(self):
        """Assign unified groups to remaining unassigned rows."""
        self.df['unified_group'] = self.df['set_coauthor_group'].fillna('')
        
        high_freq_by_group = self._calculate_high_freq_authors('set_coauthor_group')
        
        self.df['author_list'] = self.df['author_list'].apply(
            lambda x: set(x) if isinstance(x, (list, set)) else set()
        )
        self.df['name_set'] = self.df['name_set'].apply(
            lambda x: set(x) if isinstance(x, (list, set)) else set()
        )
        
        mask = self.df['set_coauthor_group'].isna() | (self.df['set_coauthor_group'] == '')
        
        self.df.loc[mask, 'unified_group'] = self.df[mask].apply(
            lambda row: self._find_best_group(row['author_list'], row['name_set'], 
                                              high_freq_by_group),
            axis=1
        )
    
    def _find_best_group(self, row_authors, row_names, high_freq_by_group):
        """Find the best matching group for a row."""
        best_group = None
        best_overlap_count = 0
        
        if len(row_authors) > 100:
            return None
        
        for group_name, group_info in high_freq_by_group.items():
            author_overlap = row_authors.intersection(group_info['high_freq_authors'])
            name_overlap = row_authors.intersection(group_info['name_set'])
            
            if len(author_overlap) >= 3 and len(name_overlap) >= 2:
                if len(author_overlap) > best_overlap_count:
                    best_group = group_name
                    best_overlap_count = len(author_overlap)
                elif len(author_overlap) == best_overlap_count and best_group is not None:
                    best_group = min(best_group, group_name)
        
        return best_group
    

    
    def create_firstlast_groups(self):
        """Create groups based on first-last author pairs."""
        df_filtered = self.df.dropna(subset=['first_author', 'last_author', 'name_set']).copy()
        df_filtered = df_filtered[df_filtered['first_author'] != df_filtered['last_author']]
        
        # Convert name_set (which is a list/set) to a hashable tuple for groupby
        df_filtered['name_set_tuple'] = df_filtered['name_set'].apply(
            lambda x: tuple(sorted(x)) if isinstance(x, (list, set)) else tuple()
        )
        
        group_keys = ['first_author', 'last_author', 'name_set_tuple']
        group_sizes = df_filtered.groupby(group_keys).size().reset_index(name='count')
        valid_groups = group_sizes[group_sizes['count'] > 2].copy()
        
        valid_groups['firstlastauthor'] = [
            f'firstlastauthor_group_{i}' for i in range(len(valid_groups))
        ]
        
        df_filtered = df_filtered.merge(valid_groups[group_keys + ['firstlastauthor']], 
                                        on=group_keys, how='left')
        
        # Add name_set_tuple to main df for merging
        self.df['name_set_tuple'] = self.df['name_set'].apply(
            lambda x: tuple(sorted(x)) if isinstance(x, (list, set)) else tuple()
        )
        
        self.df = self.df.merge(
            df_filtered[['first_author', 'last_author', 'name_set_tuple', 'firstlastauthor']],
            on=['first_author', 'last_author', 'name_set_tuple'],
            how='left'
        )
        
        # Drop the temporary column
        self.df.drop(columns=['name_set_tuple'], inplace=True)
        
        self.df['firstlastauthor'] = self.df['firstlastauthor'].fillna("")
        
        # Convert unhashable columns to strings temporarily for drop_duplicates
        self.df['name_set_str'] = self.df['name_set'].apply(str)
        self.df['author_list_str'] = self.df['author_list'].apply(str)
        
        # Drop duplicates
        #self.df = self.df.drop_duplicates()
        dedupe_subset = [c for c in self.df.columns if c not in ['name_set', 'author_list']]
        self.df = self.df.drop_duplicates(subset=dedupe_subset)
        
        # Drop temporary string columns
        self.df.drop(columns=['name_set_str', 'author_list_str'], inplace=True)
    
    

    def merge_unified_with_firstlast(self):
        """Merge unified groups with first-last author groups."""
        self.df['unified_group_all'] = self.df['unified_group'].fillna("")
        
        df_filtered = self.df[
            #self.df['unified_group'].notna() & (self.df['unified_group'].str.strip() != '') &
            self.df['unified_group'].notna() & (self.df['unified_group'].astype(str).str.strip() != '') &
            #self.df['firstlastauthor'].notna() & (self.df['firstlastauthor'].str.strip() != '')
            self.df['firstlastauthor'].notna() & (self.df['firstlastauthor'].astype(str).str.strip() != '')
        ]
        
        grouped = df_filtered.groupby('firstlastauthor')['unified_group'].unique().reset_index()
        unique_unified_groups = grouped[grouped['unified_group'].apply(len) == 1]
        
        author_to_groups = dict(zip(unique_unified_groups['firstlastauthor'], 
                                   unique_unified_groups['unified_group']))
        
        def update_unified_group_all(row):
            if row['firstlastauthor'] in author_to_groups:
                return ','.join(sorted(author_to_groups[row['firstlastauthor']]))
            else:
                return row['unified_group_all']
        
        self.df['unified_group_all'] = self.df.apply(update_unified_group_all, axis=1)
        
        # Convert unhashable columns to strings temporarily for drop_duplicates
        self.df['name_set_str'] = self.df['name_set'].apply(str)
        self.df['author_list_str'] = self.df['author_list'].apply(str)
        
        # Drop duplicates
        #self.df = self.df.drop_duplicates()
        dedupe_subset = [c for c in self.df.columns if c not in ['name_set', 'author_list']]
        self.df = self.df.drop_duplicates(subset=dedupe_subset)
        
        # Drop temporary string columns
        self.df.drop(columns=['name_set_str', 'author_list_str'], inplace=True)
    


    def find_missing_coauthor_groups(self):
        """Find groups based on shared coauthors for unassigned rows."""
        df_filtered = self.df[self.df["unified_group_all"] == ""].copy()
        
        self.df["unified_group_all_other"] = None
        self.df["new_common"] = None
        
        group_counter = 0
        
        for fname, group in df_filtered.groupby("first_name"):
            group = group.copy()
            n = len(group)
            assigned = set()
            rows = group.index.tolist()
            
            for i in range(n):
                if rows[i] in assigned:
                    continue
                
                group_members = [rows[i]]
                author_list_val = self.df.loc[rows[i], "author_list"]
                
                # Handle different types for author_list
                if isinstance(author_list_val, str) and author_list_val.startswith("{"):
                    common_authors = ast.literal_eval(author_list_val)
                elif isinstance(author_list_val, (list, set)):
                    common_authors = set(author_list_val)
                else:
                    continue
                
                for j in range(i + 1, n):
                    if rows[j] in assigned:
                        continue
                    
                    compare_list_val = self.df.loc[rows[j], "author_list"]
                    
                    # Handle different types for author_list
                    if isinstance(compare_list_val, str) and compare_list_val.startswith("{"):
                        compare_authors = ast.literal_eval(compare_list_val)
                    elif isinstance(compare_list_val, (list, set)):
                        compare_authors = set(compare_list_val)
                    else:
                        continue
                    
                    shared = common_authors & compare_authors
                    
                    if len(shared) >= 3:
                        group_members.append(rows[j])
                        common_authors &= shared
                
                if len(group_members) > 2:
                    group_counter += 1
                    for idx in group_members:
                        self.df.at[idx, "unified_group_all_other"] = f'missing_group_{group_counter}'
                        self.df.at[idx, "new_common"] = list(common_authors)
                        assigned.add(idx)


    
    def create_final_unified_groups(self):
        """Create final unified groups by merging all group types."""
        def get_frequent_authors(df, group_col, group_name, author_col="author_list", min_count=1):
            sub_df = df[df[group_col] == group_name].copy()
            sub_df[author_col] = sub_df[author_col].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("{") else x
            )
            all_authors = [
                author
                for author_list in sub_df[author_col]
                if isinstance(author_list, (set, list))
                for author in author_list
            ]
            author_counts = Counter(all_authors)
            result_df = pd.DataFrame(author_counts.items(), columns=["Author", "Count"])
            result_df = result_df[result_df["Count"] >= min_count]
            result_df = result_df.sort_values(by="Count", ascending=False).reset_index(drop=True)
            return result_df
        
        def get_unique_names(df, group_col, group_name, names_col="name_set"):
            sub_df = df[df[group_col] == group_name].copy()
            sub_df[names_col] = sub_df[names_col].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("{") else x
            )
            unique_names = set(
                name
                for names_list in sub_df[names_col]
                if isinstance(names_list, (set, list))
                for name in names_list
            )
            return unique_names
        
        unique_original_groups = self.df["unified_group_all"].dropna().unique()
        unique_other_groups = self.df["unified_group_all_other"].dropna().unique()
        
        main_authors_dict = {}
        main_names_dict = {}
        other_authors_dict = {}
        other_names_dict = {}
        
        for main_group in unique_original_groups:
            main_authors_dict[main_group] = get_frequent_authors(self.df, "unified_group_all", main_group)
            main_names_dict[main_group] = get_unique_names(self.df, "unified_group_all", main_group)
        
        for other_group in unique_other_groups:
            other_authors_dict[other_group] = get_frequent_authors(self.df, "unified_group_all_other", other_group)
            other_names_dict[other_group] = get_unique_names(self.df, "unified_group_all_other", other_group)
        
        group_links = []
        for mg in unique_original_groups:
            if 'connected_group' not in mg:
                continue
            for og in unique_other_groups:
                overlap_name = main_names_dict[mg].intersection(other_names_dict[og])
                main_authors_filtered = set(main_authors_dict[mg][main_authors_dict[mg]['Count'] >= 2]['Author'])
                other_authors_filtered = set(other_authors_dict[og][other_authors_dict[og]['Count'] >= 2]['Author'])
                overlap_authors = main_authors_filtered.intersection(other_authors_filtered)
                
                if overlap_name and len(overlap_authors) > 3:
                    group_links.append((mg, og))
        
        clusters = self._build_group_clusters(group_links)
        
        group_to_unified = {}
        for idx, cluster in enumerate(clusters, start=1):
            unified_label = f"unified_group_{idx}"
            for group in cluster:
                group_to_unified[group] = unified_label
        
        all_groups = pd.Series(pd.concat([self.df["unified_group_all"], 
                                         self.df["unified_group_all_other"]])).dropna().unique()
        
        next_index = len(set(group_to_unified.values())) + 1
        
        for group in all_groups:
            if group not in group_to_unified and group != "":
                group_to_unified[group] = f"{self.last_name}_{next_index}"
                next_index += 1
        
        def assign_unified_group(row):
            group1 = row.get("unified_group_all")
            group2 = row.get("unified_group_all_other")
            if group1 in group_to_unified:
                return group_to_unified[group1]
            elif group2 in group_to_unified:
                return group_to_unified[group2]
            else:
                return None
        
        self.df["unified_group_new"] = self.df.apply(assign_unified_group, axis=1)
        self.df.loc[self.df['unified_group_new'] == 'unified_group_1', 'unified_group_new'] = f'{self.last_name}_0'
    


    def _build_group_clusters(self, group_links):
        """Build connected components from group links."""
        graph = defaultdict(set)
        for g1, g2 in group_links:
            graph[g1].add(g2)
            graph[g2].add(g1)
        
        visited = set()
        clusters = []
        for node in graph:
            if node not in visited:
                cluster = set()
                queue = deque([node])
                while queue:
                    curr = queue.popleft()
                    if curr in visited:
                        continue
                    visited.add(curr)
                    cluster.add(curr)
                    queue.extend(graph[curr] - visited)
                clusters.append(cluster)
        return clusters
    
    
    
    def finalize_groups(self):
        """Apply containment analysis to finalize groups."""
        def get_frequent_authors(df, group_col, group_name, author_col="author_list", min_count=1):
            sub_df = df[df[group_col] == group_name].copy()
            sub_df[author_col] = sub_df[author_col].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("{") else x
            )
            all_authors = [
                author.lower().replace("-", " ")
                for author_list in sub_df[author_col]
                if isinstance(author_list, (set, list))
                for author in author_list
            ]
            author_counts = Counter(all_authors)
            result_df = pd.DataFrame(author_counts.items(), columns=["Author", "Count"])
            result_df = result_df[result_df["Count"] >= min_count]
            result_df = result_df.sort_values(by="Count", ascending=False).reset_index(drop=True)
            return result_df
        
        def get_unique_names(df, group_col, group_name, names_col="name_set"):
            sub_df = df[df[group_col] == group_name].copy()
            sub_df[names_col] = sub_df[names_col].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("{") else x
            )
            unique_names = set(
                name
                for names_list in sub_df[names_col]
                if isinstance(names_list, (set, list))
                for name in names_list
            )
            return unique_names
        
        self.df["final"] = self.df["unified_group_new"]
        unique_groups = self.df["unified_group_new"].dropna().unique()
        
        group_to_authors = {}
        group_to_names = {}
        
        for group in unique_groups:
            freq_authors_df = get_frequent_authors(self.df, "unified_group_new", group, "author_list", min_count=2)
            group_to_authors[group] = set(freq_authors_df["Author"])
            group_to_names[group] = get_unique_names(self.df, "unified_group_new", group)
        
        containment_counts = defaultdict(list)
        
        for group_a, authors_a in group_to_authors.items():
            names_a = group_to_names.get(group_a, set())
            
            for group_b, authors_b in group_to_authors.items():
                if group_a == group_b:
                    continue
                
                names_b = group_to_names.get(group_b, set())
                name_overlap = names_a.intersection(names_b)
                
                if (authors_b and authors_b.issubset(authors_a) and 
                    len(name_overlap) >= 2 and len(authors_b) >= 2):
                    containment_counts[group_a].append(group_b)
        
        contained_to_container = {}
        for container, contained_list in containment_counts.items():
            for contained in contained_list:
                contained_to_container[contained] = container
        
        self.df["final"] = self.df["final"].apply(lambda x: contained_to_container.get(x, x))
        #self.df["final_dup"] = self.df["final"]


    
    def process(self, output_csv: str = None):
        """
        Main processing pipeline.
        
        Args:
            output_csv (str, optional): Path to output CSV file
        
        Returns:
            pd.DataFrame: Processed dataframe with disambiguation results
        """
        start_time = time.time()
        
        self.logger.info(f"Starting author disambiguation for: {self.last_name}")
        #print(f"{'='*60}\n")
        
        # Step 1: Initial grouping 
        #print("Step 1: Creating initial groups (ORCID, affiliation, coauthor)...")
        self.process_initial_grouping()
        
        # Step 2: Resolve conflicts 
        #print("Step 2: Resolving conflicts...")
        self.resolve_conflicts()
        
        # Step 3: Merge groups 
        #print("Step 3: Merging groups...")
        self.create_merged_groups()
        
        # Step 4: Refine with coauthors 
        #print("Step 4: Refining with coauthor analysis...")
        self.refine_with_coauthors()
        
        # Step 5: Assign unified groups 
        #print("Step 5: Assigning unified groups...")
        self.assign_unified_groups()
        
        # Step 6: First-last author groups 
        #print("Step 6: Creating first-last author groups...")
        self.create_firstlast_groups()
        
        # Step 7: Merge unified with first-last 
        #print("Step 7: Merging unified with first-last groups...")
        self.merge_unified_with_firstlast()
        
        # Step 8: Find missing coauthor groups 
        #print("Step 8: Finding missing coauthor groups...")
        self.find_missing_coauthor_groups()
        
        # Step 9: Create final unified groups 
        #print("Step 9: Creating final unified groups...")
        self.create_final_unified_groups()
        
        # Step 10: Finalize 
        #print("Step 10: Finalizing groups...")
        self.finalize_groups()
        
        # Step 11: Clean up for output 
        #print("Step 11: Preparing output...")
        self.df["author_list"] = self.df["author_list"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("{") else x
        )

        self.df["author_list"] = self.df["author_list"].apply(
            lambda x: len(x) if isinstance(x, (list, set)) and len(x) > 100 else x
        )

        self.df["new_common"] = self.df["new_common"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x
        )

        self.df["new_common"] = self.df["new_common"].apply(
            lambda x: len(x) if isinstance(x, (list, set)) and len(x) > 100 else x
        )
        
        # Save output if path provided
        if output_csv:
            #print(f"Step 12: Saving to {output_csv}...")
            self.df.to_csv(output_csv, index=False)
            self.logger.info(f"💾 Output saved to: {output_csv}")
        

        end_time = time.time()
        hours, minutes, seconds = _elapsed_time(start_time, end_time)
 
        total = len(self.df)
        unique = len(self.df['final'].unique())
        numOfIdentified = self.df['final'].notna().sum()  # counts non-null values

        if hours > 0 or minutes > 0 or seconds > 10:            
            self.logger.info(f"✅ Time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds.  📊 Processed {total} records")

        self.logger.info(f"👥 Unique disambiguated identities: {unique}. Total {numOfIdentified} of {total} have been identified")

        return self.df


if __name__ == '__main__':
    # Example usage
    print("Author Disambiguation System")
    print("=" * 60)
    print("\nThis system accepts a list of dictionaries with author data.")
    print("\nExpected fields:")
    print("  - id, associate_id, associate_type, source")
    print("  - first_name, last_name, affiliation, orcid")
    print("  - email, phone, first_publication_date, title")
    print("  - abstract_text, author_list, first_author, last_author")
    print("\nUsage in Python:")
    print("  from author_disambiguation import disambiguate_authors")
    print("  ")
    print("  data = [")
    print("      {")
    print("          'id': 1,")
    print("          'first_name': 'John',")
    print("          'last_name': 'Smith',")
    print("          'orcid': '0000-0001-2345-6789',")
    print("          'affiliation': 'MIT',")
    print("          'author_list': 'John Smith, Jane Doe, Bob Johnson',")
    print("          'first_author': 'John Smith',")
    print("          'last_author': 'Bob Johnson',")
    print("          ...")
    print("      },")
    print("      ...")
    print("  ]")
    print("  ")
    print("  # Process and get results")
    print("  result_df = disambiguate_authors(data)")
    print("  ")
    print("  # Process and save to CSV")
    print("  result_df = disambiguate_authors(data, 'output.csv')")
    print("\n" + "=" * 60)

    import csv

    with open('Moon-2025-09-22.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        data_list = list(csv_reader)

    last_name = 'Moon'
    disambiguator = PersonDisambiguator(last_name,data_list)
    
    disambiguator.process("out-put.csv")
