"""
Extract node and mapping files from the GSE tables

"""
import os
import pandas as pd
import traceback
import ast


##################
# Platform node with platform id.
##################
def generate_platform_node(input_path, platform_table, output_path):
    """
    Extract Platform information, deduplicate, and assign unique Platform IDs.

    Args:
        input_path (str): Path to the folder containing CSV files organized in subfolders.
        platform_table (pd.DataFrame): Existing platform table to check for duplicates.

    Returns:
        pd.DataFrame: Updated platform table with unique Platform IDs.
    """

    existing_platforms = set(
        tuple(x) for x in platform_table[['Platform_name', 'Platform_manufacturer']].dropna().to_numpy()
    )

    #platform_name_set = set()
    #platform_manufacturer_set = set()
    
    # Iterate through each subfolder
    for subfolder in os.listdir(input_path):
        subfolder_path = os.path.join(input_path, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        # Iterate through each CSV file in the subfolder
        for csv_file in os.listdir(subfolder_path):
            #if not csv_file.endwith('.csv'):
            #    continue
            file_path = os.path.join(subfolder_path, csv_file)
            df = pd.read_csv(file_path)

            # Filter rows where Primary_category = 'Platform'
            platform_rows = df[df['Primary_category'] == 'Platform']
            # Check if the DataFrame has required data
            if platform_rows.empty or 'Attribute' not in platform_rows.columns or 'Content' not in platform_rows.columns:
                print(f"Skipping {csv_file}: Missing required data or empty DataFrame.")
                continue

            
            try:
                # Extract Platform_name and Platform_manufacturer rows
                platform_name_row = platform_rows[platform_rows['Attribute'] == 'Platform_name']
                platform_manufacturer_row = platform_rows[platform_rows['Attribute'] == 'Platform_manufacturer']

                # Extract and normalize Platform_name
                platform_name = None
                if not platform_name_row.empty:
                    content = platform_name_row['Content'].iloc[0]
                    try:
                        platform_name_list = ast.literal_eval(content)
                        if isinstance(platform_name_list, list) and platform_name_list:
                            platform_name = platform_name_list[0].strip() if platform_name_list[0] else None
                    except (ValueError, SyntaxError):
                        platform_name = None

                # Extract and normalize Platform_manufacturer
                platform_manufacturer = None
                if not platform_manufacturer_row.empty:
                    content = platform_manufacturer_row['Content'].iloc[0]
                    try:
                        platform_manufacturer_list = ast.literal_eval(content)
                        if isinstance(platform_manufacturer_list, list) and platform_manufacturer_list:
                            platform_manufacturer = platform_manufacturer_list[0].strip() if platform_manufacturer_list[0] else None
                    except (ValueError, SyntaxError):
                        platform_manufacturer = None

                # Skip if both values are None
                if platform_name is None and platform_manufacturer is None:
                    print(f"Skipping {csv_file}: Both platform_name and platform_manufacturer are None.")
                    continue

                # Check for duplicates
                if (platform_name, platform_manufacturer) in existing_platforms:
                    continue

                # Add to platform table and track the new combination
                existing_platforms.add((platform_name, platform_manufacturer))
                platform_table = pd.concat(
                    [platform_table, pd.DataFrame([{
                        'Platform_name': platform_name,
                        'Platform_manufacturer': platform_manufacturer
                    }])],
                    ignore_index=True
                )
            except Exception as e:
                print(f"Error processing {csv_file}: {e}")
                traceback.print_exc()  # Add full traceback for debugging
                print("Debugging platform_rows DataFrame:")
                print(platform_rows['Attribute'], " ", platform_rows['Content'])

    # Generate unique Platform IDs for the updated table
    platform_table['Platform_id'] = [f"GEO-PLT-{str(i).zfill(5)}" for i in range(len(platform_table))]

    # Save to CSV
    platform_table.to_csv(os.path.join(output_path, 'platform_node.csv'), index=False)
    return platform_table
                

##################
# Experiment node with ids for mapping relationships
##################
def generate_experiment_node(input_path, platform_table, output_path):
    # Initialize a list
    experiment_data = []
    # Track seen External_experiment_source_id values
    seen_experiment_ids = set()

    for subfolder in os.listdir(input_path):
        subfolder_path = os.path.join(input_path, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        for csv_file in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, csv_file)
            df = pd.read_csv(file_path)

            try:
                # Extract relevant rows for the Experiment node
                relevant_attributes = [
                    'Experiment_data_link', 'Omics_type', 'Sequencing_type', 'Sequencing_library',
                    'External_experiment_source_id', 'External_project_source_id',
                    'Platform_name', 'Platform_manufacturer'
                ]
                relevant_rows = df[df['Attribute'].isin(relevant_attributes)]

                experiment_row = {}

                # Extract values for relevant attributes
                for attribute in relevant_attributes:
                    attribute_row =relevant_rows[relevant_rows['Attribute'] == attribute]
                    if not attribute_row.empty:
                        # Extract the entire list from the Content column
                        content_value = (
                            eval(attribute_row['Content'].iloc[0])
                            if isinstance(attribute_row['Content'].iloc[0], str) and attribute_row['Content'].iloc[0].startswith('[')
                            else None
                        )
                        # Handle `[None]` in Content by converting it to None
                        content_value = [x if x is not None else "" for x in content_value] if isinstance(content_value, list) else content_value
                        experiment_row[attribute] = content_value
                    else:
                        experiment_row[attribute] = None  # Add None if the attribute is missing

                # Skip if experiement already exists
                External_experiment_source_id = experiment_row.get('External_experiment_source_id')
                if isinstance(External_experiment_source_id, list):
                    External_experiment_source_id = External_experiment_source_id[0] if External_experiment_source_id else None
                if External_experiment_source_id in seen_experiment_ids:
                    continue # Skip duplicate
                seen_experiment_ids.add(External_experiment_source_id)


                # Match Platform_name and Platform_manufacturer with platform_table
                platform_name = experiment_row.get('Platform_name')
                platform_manufacturer = experiment_row.get('Platform_manufacturer')

                # Initialize platform_ids as an empty list
                platform_ids = []

                # Handle case where we have multiple platforms
                if platform_name and isinstance(platform_name, list) and len(platform_name) > 0:
                    # For each platform name, try to find a match
                    for i, p_name in enumerate(platform_name):
                        # Get corresponding manufacturer if available
                        p_manufacturer = ""
                        if platform_manufacturer and isinstance(platform_manufacturer, list) and i < len(platform_manufacturer):
                            p_manufacturer = platform_manufacturer[i]
                        
                        # Look for a match in platform_table
                        match = platform_table[
                            (platform_table['Platform_name'].fillna("") == p_name) &
                            (platform_table['Platform_manufacturer'].fillna("") == p_manufacturer)
                        ]
                        if not match.empty:
                            platform_ids.append(match['Platform_id'].iloc[0])
                else:
                    # Handle non-list values (single string, None, etc.)
                    p_name = platform_name if platform_name else ""
                    p_manufacturer = platform_manufacturer if platform_manufacturer else ""
                    
                    # If they're strings but not lists, use them directly
                    match = platform_table[
                        (platform_table['Platform_name'].fillna("") == p_name) &
                        (platform_table['Platform_manufacturer'].fillna("") == p_manufacturer)
                    ]
                    if not match.empty:
                        platform_ids.append(match['Platform_id'].iloc[0])

                # Add Platform_id to the row (as a list)
                experiment_row['Platform_id'] = platform_ids if platform_ids else None
                # Remove Platform_name and Platform_manufacturer
                experiment_row.pop('Platform_name', None)
                experiment_row.pop('Platform_manufacturer', None)

                # Append the row to the experiment data
                experiment_data.append(experiment_row)

            except Exception as e:
                print(f"Error processing {csv_file}: {e}")
                traceback.print_exc()
                print("Debugging relevant_rows DataFrame:")
                print(relevant_rows)
                continue

    # Convert to DataFrame
    experiment_df = pd.DataFrame(experiment_data)

    # Generate unique Experiment_id
    experiment_df['Experiment_id'] = [f"GEO-EXPT-{str(i).zfill(5)}" for i in range(len(experiment_df))]

    # Save to CSV
    output_file = os.path.join(output_path, 'experiment_node.csv')
    experiment_df.to_csv(output_file, index=False)
    print(f"Experiment node data saved to {output_file}.")
    return experiment_df

##################
# Sample node and ids for mapping relationships
##################
def generate_sample_node(input_path, experiment_node_path, output_path):
    # Load experiment_node.csv
    experiment_node = pd.read_csv(experiment_node_path)

    # Normalize External_experiment_source_id in experiment_node.csv
    experiment_node['External_experiment_source_id'] = experiment_node['External_experiment_source_id'].apply(
        lambda x: eval(x)[0] if isinstance(x, str) and x.startswith('[') else x
    )


    # Initialize a list to store sample data
    sample_data = []
    unique_samples = set() # Track unique External_sample_source_id
    for subfolder in os.listdir(input_path):
        subfolder_path = os.path.join(input_path, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        for csv_file in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, csv_file)
            df = pd.read_csv(file_path)

            try:
                # Extract External_experiment_source_id
                experiment_row = df[(df['Primary_category'] == 'Experiment') &
                                    (df['Attribute'] == 'External_experiment_source_id')]
                
                external_experiment_id = eval(experiment_row['Content'].iloc[0])[0]
                # Filter rows where Primary_category == 'Sample'
                sample_rows = df[df['Primary_category'] == 'Sample']

                # Determine the length of the External_sample_source_id list
                external_sample_source_id_row = sample_rows[sample_rows['Attribute'] == 'External_sample_source_id']
                if not external_sample_source_id_row.empty:
                    external_sample_source_id_list = eval(external_sample_source_id_row['Content'].iloc[0])
                    num_samples = len(external_sample_source_id_list)  # Length of the list
                else:
                    num_samples = 0  # Default to 0 if no External_sample_source_id is found


                # Create a dictionary for each sample
                sample_dict = {attribute: [] for attribute in sample_rows['Attribute'].unique()}
                sample_dict['External_experiment_source_id'] = [external_experiment_id] * num_samples# Add the experiment ID

                for _, row in sample_rows.iterrows():
                    attribute = row['Attribute']
                    content = (
                        eval(row['Content'])
                        if isinstance(row['Content'], str) and row['Content'].startswith('[')
                        else []
                    )

                    # Handle None or empty values
                    if not content:
                        content = ["" for _ in range(num_samples)]
                    sample_dict[attribute] = content
                # Normalize lengths of all attributes
                max_length = max([len(values) for values in sample_dict.values()])
                for key in sample_dict:
                    while len(sample_dict[key]) < max_length:
                        sample_dict[key].append("")

                # Add data to the sample_data list
                for i in range(max_length):
                    external_sample_id = sample_dict.get('External_sample_source_id', [])[i]
                    if external_sample_id not in unique_samples:
                        unique_samples.add(external_sample_id)
                        sample_data.append({key: sample_dict[key][i] for key in sample_dict})

            except Exception as e:
                print(f"Error processing {csv_file}: {e}")
                traceback.print_exc()
                print("Debugging sample_rows DataFrame:")
                print(sample_rows)
                continue
    
    # Convert to DataFrame
    sample_df = pd.DataFrame(sample_data)

    # Add Platform_id and Experiment_id by mapping from experiment_node.csv
    experiment_node['External_experiment_source_id'] = experiment_node['External_experiment_source_id'].fillna("").astype(str)
    sample_df = sample_df.merge(
        experiment_node[['External_experiment_source_id', 'Platform_id', 'Experiment_id']],
        on='External_experiment_source_id',
        how='left'
    )


    # Generate unique Sample_id
    sample_df['Sample_id'] = [f"GEO-SAMPLE-{str(i).zfill(5)}" for i in range(len(sample_df))]

    # Save to CSV
    output_file = os.path.join(output_path, 'sample_node.csv')
    sample_df.to_csv(output_file, index=False)
    print(f"Sample node data saved to {output_file}.")
    return sample_df

##################
# Project node and ids for mapping relationships
#1. Use a dictionary project_map instead of a set.

#2. Key it by External_project_source_id.

#3.For each file:
#   If the project is new, create a new entry with all info, and initialize a list of GardIds.
#   If the project is already seen, check if the new GardId is not already in the list, and if not, append it.
##################
def generate_project_node(input_path, output_path):
    # Dictionary to store unique projects with GardId as list
    project_map = {}

    for subfolder in os.listdir(input_path):
        subfolder_path = os.path.join(input_path, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        for csv_file in os.listdir(subfolder_path):
            if not csv_file.endswith('.csv'):
                continue

            file_path = os.path.join(subfolder_path, csv_file)
            df = pd.read_csv(file_path)

            try:
                # Extract project-related rows
                project_rows = df[df['Primary_category'] == 'Project']
                if project_rows.empty:
                    print(f"No Project data in {csv_file}. Skipping...")
                    continue

                # Create a dictionary for the project
                project_dict = {}
                for _, row in project_rows.iterrows():
                    attribute = row['Attribute']
                    content = row['Content']

                    # Handle empty or NaN content
                    if isinstance(content, str) and content.startswith('['):
                        try:
                            content_list = eval(content)
                            content = content_list[0] if content_list else ""
                        except Exception:
                            content = ""
                    else:
                        content = ""
                    project_dict[attribute] = content
                
                external_project_id = project_dict.get('External_project_source_id', "")
                if not external_project_id:
                    print(f"Missing External_project_source_id in {csv_file}. Skipping...")
                    continue

                # Extract GardId from Condition
                gard_id = None
                condition_row = df[(df['Primary_category'] == 'Condition') & (df['Attribute'] == 'GardId')]
                if not condition_row.empty:
                    gard_content = condition_row['Content'].iloc[0]
                    if isinstance(gard_content, str) and gard_content.startswith('['):
                        gard_list = eval(gard_content)
                        gard_id = gard_list[0] if gard_list else None

                #Add or update project
                if external_project_id in project_map:
                    if gard_id and gard_id not in project_map[external_project_id]['GardId']:
                        project_map[external_project_id]['GardId'].append(gard_id)
                else:
                    project_dict['GardId'] = [gard_id] if gard_id else []
                    project_map[external_project_id] = project_dict


            except Exception as e:
                print(f"Error processing {csv_file}: {e}")
                traceback.print_exc()
                print("Debugging project_rows DataFrame:")
                print(project_rows)
                continue

    # Convert to DataFrame
    project_data = list(project_map.values())
    project_df = pd.DataFrame(project_data)

    # Generate unique Project_id
    project_df['Project_id'] = [f"GEO-PROJ-{str(i).zfill(5)}" for i in range(len(project_df))]

    # Save to CSV
    output_file = os.path.join(output_path, 'project_node.csv')
    project_df.to_csv(output_file, index=False)
    print(f"Project node data saved to {output_file}.")
    return project_df
    
##################
# Update experiment_node.csv
##################
def update_experiment_node(experiment_node_path, project_node_path, output_path):
    try:
        # Read experiment_node.csv and project_node.csv
        experiment_node = pd.read_csv(experiment_node_path)
        project_node = pd.read_csv(project_node_path)
        

        # Check if required columns exist
        if 'External_project_source_id' not in experiment_node.columns:
            raise ValueError("Experiment table must contain 'External_project_source_id' column.")
        if 'External_project_source_id' not in project_node.columns or 'Project_id' not in project_node.columns:
            raise ValueError("Project table must contain 'External_project_source_id' and 'Project_id' columns.")

        # Normalize External_project_source_id in experiment_node
        def normalize_external_project_id(value):
            if isinstance(value, str) and value.startswith('['):
                try:
                    value_list = eval(value)
                    return value_list[0] if value_list else ""
                except Exception as e:
                    print(f"Error normalizing value: {value}. Exception: {e}")
                    return ""
            return value

        experiment_node['External_project_source_id'] = experiment_node['External_project_source_id'].apply(
            normalize_external_project_id
        )

        # Normalize External_project_source_id in project_node
        project_node['External_project_source_id'] = project_node['External_project_source_id'].astype(str)


        # Simplify Sequencing_type and Sequencing_library
        for column in ['Sequencing_type', 'Sequencing_library']:
            if column in experiment_node.columns:
                experiment_node[column] = experiment_node[column].apply(
                    lambda x: simplify_list_column(x)
                )
        # Remove any existing Project_id_x or Project_id_y columns to avoid confusion
        #if 'Project_id_x' in experiment_node.columns:
        #    experiment_node.drop(columns=['Project_id_x'], inplace=True)
        #if 'Project_id_y' in experiment_node.columns:
        #    experiment_node.drop(columns=['Project_id_y'], inplace=True)
        # Merge Project_id into experiment_node
        experiment_node = experiment_node.merge(
            project_node[['External_project_source_id', 'Project_id']],
            on='External_project_source_id',
            how='left'
        )

        # Save the updated experiment_node.csv
        output_file = os.path.join(output_path, 'experiment_node.csv')
        experiment_node.to_csv(output_file, index=False)
        print(f"Updated experiment_node.csv saved to {output_file}.")
        return experiment_node

    except Exception as e:
        print(f"Error updating experiment_node.csv with Project_id: {e}")
        traceback.print_exc()

# Helper function to simplify list columns
def simplify_list_column(value):
    try:
        if isinstance(value, str) and value.startswith('['):
            value_list = eval(value)
            # Deduplicate elements and convert back to a string
            unique_values = sorted(set(value_list))
            if len(unique_values) == 1:
                return unique_values[0]  # If all elements are the same, keep one
            return str(unique_values)  # If different, keep unique elements as a list
        return value
    except Exception as e:
        print(f"Error simplifying column value: {value}. Exception: {e}")
        return value

##################
# Generate Condition node
##################
def generate_condition_node(input_path, output_path):
    try:
        # Initialize a list to store condition data
        condition_data = []
        unique_conditions = set()  # Track unique GardId values to avoid duplicates

        for subfolder in os.listdir(input_path):
            subfolder_path = os.path.join(input_path, subfolder)
            if not os.path.isdir(subfolder_path):
                continue

            for csv_file in os.listdir(subfolder_path):
                if not csv_file.endswith('.csv'):
                    continue

                file_path = os.path.join(subfolder_path, csv_file)
                df = pd.read_csv(file_path)

                try:
                    # Extract condition-related rows
                    condition_rows = df[df['Primary_category'] == 'Condition']
                    if condition_rows.empty:
                        print(f"No Condition data in {csv_file}. Skipping...")
                        continue

                    # Create a dictionary for each condition
                    condition_dict = {}
                    for _, row in condition_rows.iterrows():
                        attribute = row['Attribute']
                        content = row['Content']

                        # Handle list-like content
                        if isinstance(content, str) and content.startswith('['):
                            try:
                                content_list = eval(content)
                                content = content_list[0] if content_list else ""
                            except Exception as e:
                                print(f"Error processing content: {content}. Exception: {e}")
                                content = ""

                        condition_dict[attribute] = content

                    # Ensure GardId is present and unique
                    gard_id = condition_dict.get('GardId', "")
                    if gard_id in unique_conditions:
                        print(f"Duplicate GardId '{gard_id}' found in {csv_file}. Skipping...")
                        continue

                    # Add the condition to the data and mark it as processed
                    unique_conditions.add(gard_id)
                    condition_data.append(condition_dict)

                except Exception as e:
                    print(f"Error processing {csv_file}: {e}")
                    traceback.print_exc()
                    print("Debugging condition_rows DataFrame:")
                    print(condition_rows)
                    

        # Convert to DataFrame
        condition_df = pd.DataFrame(condition_data)

        # Generate unique Condition_id
        #condition_df['Condition_id'] = [f"GEO-COND-{str(i).zfill(5)}" for i in range(len(condition_df))]

        # Save to CSV
        output_file = os.path.join(output_path, 'condition_node.csv')
        condition_df.to_csv(output_file, index=False)
        print(f"Condition node data saved to {output_file}.")
        return condition_df

    except Exception as e:
        print(f"Error generating condition_node.csv: {e}")
        traceback.print_exc()

##################
# Generate publication node
##################
def generate_publication_node(input_path, project_node_path, output_path):
    try:
        # Initialize a list to store publication data
        publication_data = []
        unique_pubmed_ids = set()  # Track unique Pubmed_id to avoid duplicates

        # Read project node data
        project_node = pd.read_csv(project_node_path)

        for subfolder in os.listdir(input_path):
            subfolder_path = os.path.join(input_path, subfolder)
            if not os.path.isdir(subfolder_path):
                continue

            for csv_file in os.listdir(subfolder_path):
                if not csv_file.endswith('.csv'):
                    continue

                file_path = os.path.join(subfolder_path, csv_file)
                df = pd.read_csv(file_path)

                try:
                    # Extract publication-related rows
                    publication_rows = df[df['Primary_category'] == 'Publication']
                    project_row = df[(df['Primary_category'] == 'Project') & (df['Attribute'] == 'External_project_source_id')]

                    if publication_rows.empty or project_row.empty:
                        print(f"No Publication or External_project_source_id data in {csv_file}. Skipping...")
                        continue

                    # Extract External_project_source_id
                    external_project_id = project_row['Content'].iloc[0]
                    if isinstance(external_project_id, str) and external_project_id.startswith('['):
                        try:
                            external_project_id_list = eval(external_project_id)
                            external_project_id = external_project_id_list[0] if external_project_id_list else ""
                        except Exception as e:
                            print(f"Error processing External_project_source_id: {external_project_id}. Exception: {e}")
                            external_project_id = ""

                    # Create a dictionary for each publication
                    publication_dict = {}
                    for _, row in publication_rows.iterrows():
                        attribute = row['Attribute']
                        content = row['Content']

                        # Handle list-like content
                        if isinstance(content, str) and content.startswith('['):
                            try:
                                content_list = eval(content)
                                content = content_list[0] if content_list else ""
                            except Exception as e:
                                print(f"Error processing content: {content}. Exception: {e}")
                                content = ""

                        publication_dict[attribute] = content

                    # Add External_project_source_id to the publication dictionary
                    publication_dict['External_project_source_id'] = external_project_id

                    # Exclude empty Pubmed_id rows
                    pubmed_id = publication_dict.get('Pubmed_id', "")
                    if not pubmed_id:
                        continue

                    # Handle multiple Pubmed_id and Project_id
                    pubmed_ids = [x.strip() for x in pubmed_id.split(",")]
                    if len(pubmed_ids) > 1:
                        for pid in pubmed_ids:
                            if pid in unique_pubmed_ids:
                                continue
                            unique_pubmed_ids.add(pid)
                            publication_data.append({
                                'Pubmed_id': pid,
                                'Authors': publication_dict.get('Authors', ""),
                                'Title': publication_dict.get('Title', ""),
                                'Journal': publication_dict.get('Journal', ""),
                                'Abstract': publication_dict.get('Abstract', ""),
                                'External_project_source_id': [external_project_id]
                            })
                    else:
                        if pubmed_ids[0] in unique_pubmed_ids:
                            # Update existing entry's Project_id list
                            for pub_entry in publication_data:
                                if pub_entry['Pubmed_id'] == pubmed_ids[0]:
                                    pub_entry['External_project_source_id'].append(external_project_id)
                                    break
                        else:
                            unique_pubmed_ids.add(pubmed_ids[0])
                            publication_data.append({
                                'Pubmed_id': pubmed_ids[0],
                                'Authors': publication_dict.get('Authors', ""),
                                'Title': publication_dict.get('Title', ""),
                                'Journal': publication_dict.get('Journal', ""),
                                'Abstract': publication_dict.get('Abstract', ""),
                                'External_project_source_id': [external_project_id]
                            })

                except Exception as e:
                    print(f"Error processing {csv_file}: {e}")
                    traceback.print_exc()
                    print("Debugging publication_rows DataFrame:")
                    print(publication_rows)
                    continue

        # Convert to DataFrame
        publication_df = pd.DataFrame(publication_data)
        
        # Explode External_project_source_id to handle lists
        publication_df = publication_df.explode('External_project_source_id')

        # Ensure all External_project_source_id are strings
        publication_df['External_project_source_id'] = publication_df['External_project_source_id'].astype(str)

        # Merge with project_node to get Project_id
        publication_df = publication_df.merge(
            project_node[['External_project_source_id', 'Project_id']],
            how='left',
            on='External_project_source_id'
        )
        # Group by Pubmed_id and combine Project_id lists
        publication_df = publication_df.groupby('Pubmed_id', as_index=False).agg({
            'Authors': 'first',
            'Title': 'first',
            'Journal': 'first',
            'Abstract': 'first',
            'Project_id': lambda x: sorted(set(str(i) for i in x if pd.notna(i)))
        })

        # Drop External_project_source_id after merging
        #publication_df = publication_df.drop(columns=['External_project_source_id'])

        # Combine Project_id lists into unique, sorted lists
        #publication_df['Project_id'] = publication_df['Project_id'].apply(lambda x: sorted(set(x)) if isinstance(x, list) else x)

        # Generate unique Publication_id
        #publication_df['Publication_id'] = [f"PUB-{str(i).zfill(5)}" for i in range(len(publication_df))]

        # Save to CSV
        output_file = os.path.join(output_path, 'publication_node.csv')
        publication_df.to_csv(output_file, index=False)
        print(f"Publication node data saved to {output_file}.")
        return publication_df

    except Exception as e:
        print(f"Error generating publication_node.csv: {e}")
        traceback.print_exc()


##################
# Generate mapping relationships csv
##################
def generate_mapping_relationships(experiment_node_path, sample_node_path, project_node_path, publication_node_path, output_path):
    try:
        
        # Read the experiment table
        experiment_node = pd.read_csv(experiment_node_path)

        # Check if the required columns exist in experiment_node
        if 'Experiment_id' not in experiment_node.columns or 'Platform_id' not in experiment_node.columns:
            raise ValueError("Experiment table must contain 'Experiment_id' and 'Platform_id' columns.")

        # Extract the Experiment-Platform mapping
        experiment_platform_mapping = []
        
        for _, row in experiment_node.iterrows():
            experiment_id = row['Experiment_id']
            platform_ids = row['Platform_id']
            
            # Parse the Platform_id column which might be stored as a string representation of a list
            if isinstance(platform_ids, str):
                try:
                    # This handles cases like "[id1, id2, id3]"
                    platform_ids = ast.literal_eval(platform_ids)
                except (ValueError, SyntaxError):
                    # If it's not a valid list representation, treat as a single value
                    platform_ids = [platform_ids] if pd.notna(platform_ids) else []
            elif not isinstance(platform_ids, list):
                # Handle non-string, non-list values
                platform_ids = [platform_ids] if pd.notna(platform_ids) else []
            
            # Create a mapping entry for each Platform_id
            for platform_id in platform_ids:
                if pd.notna(platform_id) and platform_id != "":  # Skip empty or NaN values
                    experiment_platform_mapping.append({
                        'Experiment_id': experiment_id,
                        'Platform_id': platform_id
                    })
        
        # Convert to DataFrame
        experiment_platform_mapping = pd.DataFrame(experiment_platform_mapping)
        
        # Save the Experiment-Platform mapping
        experiment_platform_file = os.path.join(output_path, 'experiment_platform_mapping.csv')
        experiment_platform_mapping.to_csv(experiment_platform_file, index=False)
        print(f"Experiment-Platform mapping saved to {experiment_platform_file}.")

        # Read the sample table
        sample_node = pd.read_csv(sample_node_path)

        # Check if the required columns exist in sample_node
        if 'Sample_id' not in sample_node.columns or 'Experiment_id' not in sample_node.columns or 'Platform_id' not in sample_node.columns:
            raise ValueError("Sample table must contain 'Sample_id', 'Experiment_id', and 'Platform_id' columns.")

        # Extract the Sample-Experiment mapping
        sample_experiment_mapping = sample_node[['Sample_id', 'Experiment_id']].dropna(subset=['Sample_id', 'Experiment_id'])

        # Save the Sample-Experiment mapping
        sample_experiment_file = os.path.join(output_path, 'sample_experiment_mapping.csv')
        sample_experiment_mapping.to_csv(sample_experiment_file, index=False)
        print(f"Sample-Experiment mapping saved to {sample_experiment_file}.")

        # Extract the Sample-Platform mapping
        sample_platform_mapping = sample_node[['Sample_id', 'Platform_id']].dropna(subset=['Sample_id', 'Platform_id'])

        # Save the Sample-Platform mapping
        sample_platform_file = os.path.join(output_path, 'sample_platform_mapping.csv')
        sample_platform_mapping.to_csv(sample_platform_file, index=False)
        print(f"Sample-Platform mapping saved to {sample_platform_file}.")
        

        # Read the project table
        project_node = pd.read_csv(project_node_path)

        # Check if the required columns exist in project_node
        if 'GardId' not in project_node.columns or 'Project_id' not in project_node.columns:
            raise ValueError("Project table must contain 'GardId' and 'Project_id' columns.")

        # Extract the Condition-Project mapping
        #condition_project_mapping = project_node[['GardId', 'Project_id']].dropna(subset=['GardId', 'Project_id'])
        # Handle GardId as a list by creating a row for each GardId-Project_id pair
        condition_project_mapping = []
        for _, row in project_node.iterrows():
            project_id = row['Project_id']
            gard_ids = row['GardId']
            
            # Parse the GardId column which is stored as a string representation of a list
            if isinstance(gard_ids, str):
                try:
                    # This handles cases like "[123, 456]" or "[]"
                    gard_ids = ast.literal_eval(gard_ids)
                except (ValueError, SyntaxError):
                    # If it's not a valid list representation, treat as a single value
                    gard_ids = [gard_ids] if pd.notna(gard_ids) else []
            elif not isinstance(gard_ids, list):
                # Handle non-string, non-list values (like a single integer)
                gard_ids = [gard_ids] if pd.notna(gard_ids) else []
                
            # Create a mapping entry for each GardId
            for gard_id in gard_ids:
                if pd.notna(gard_id) and gard_id != "":  # Skip empty or NaN values
                    condition_project_mapping.append({
                        'GardId': gard_id,
                        'Project_id': project_id
                    })
        # Convert to DataFrame
        condition_project_mapping = pd.DataFrame(condition_project_mapping)

        # Save the Condition-Project mapping
        condition_project_file = os.path.join(output_path, 'condition_project_mapping.csv')
        condition_project_mapping.to_csv(condition_project_file, index=False)
        print(f"Condition-Project mapping saved to {condition_project_file}.")
        
        experiment_node = pd.read_csv(experiment_node_path)

        # Extract the Project-Experiment mapping
        project_experiment_mapping = experiment_node[['Project_id', 'Experiment_id']].dropna(subset=['Project_id', 'Experiment_id'])

        # Save the Project-Experiment mapping
        project_experiment_file = os.path.join(output_path, 'project_experiment_mapping.csv')
        project_experiment_mapping.to_csv(project_experiment_file, index=False)
        print(f"Project-Experiment mapping saved to {project_experiment_file}.")
        


        # Read the publication table
        publication_node = pd.read_csv(publication_node_path)

        # Check if the required columns exist in publication_node
        if 'Pubmed_id' not in publication_node.columns or 'Project_id' not in publication_node.columns:
            raise ValueError("Publication table must contain 'Pubmed_id' and 'Project_id' columns.")

        # Expand Project_id lists to 1-to-1 mapping
        publication_project_mapping = []
        for _, row in publication_node.iterrows():
            pubmed_id = row['Pubmed_id']
            project_ids = eval(row['Project_id']) if isinstance(row['Project_id'], str) else []
            for project_id in project_ids:
                publication_project_mapping.append({'Pubmed_id': pubmed_id, 'Project_id': project_id})

        # Convert to DataFrame
        publication_project_df = pd.DataFrame(publication_project_mapping)

        # Save the Publication-Project mapping
        publication_project_file = os.path.join(output_path, 'publication_project_mapping.csv')
        publication_project_df.to_csv(publication_project_file, index=False)
        print(f"Publication-Project mapping saved to {publication_project_file}.")




    except Exception as e:
        print(f"Error generating mapping relationships: {e}")
        traceback.print_exc()



     




# Total tables: 2675
def count_total_files(input_path):
    count = 0
    for subfolder in os.listdir(input_path):
        subfolder_path = os.path.join(input_path, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        for csv_file in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, csv_file)
            count += 1
    print("Total tables:", count)
    




# Main processing function
def process_all_nodes(input_path, output_path):
    os.makedirs(output_path, exist_ok=True)
    
    # Generate Platform node
    platform_table = pd.DataFrame(columns=['Platform_name', 'Platform_manufacturer'])
    platform_table = generate_platform_node(input_path, platform_table, output_path)
    
    # Generate Experiment node
    platform_table_path = os.path.join(output_path, 'platform_node.csv')
    platform_table = pd.read_csv(platform_table_path)
    generate_experiment_node(input_path, platform_table, output_path)

    # Generate Sample node
    experiment_node_path = os.path.join(output_path, 'experiment_node.csv')
    generate_sample_node(input_path, experiment_node_path, output_path)

    # Generate Project node
    generate_project_node(input_path, output_path)

    # Updating the Experiment node & get Project-Experiemtn mapping
    # 1.Replace External_project_source_id with Project_id from Project node. 2.Shortening the list of sequencing type and library by deduplicates.
    project_node_path = os.path.join(output_path, 'project_node.csv')
    update_experiment_node(experiment_node_path, project_node_path, output_path)

    # Generate Condition node
    generate_condition_node(input_path, output_path)

    # Generate Publication node & Publication-Project mapping
    generate_publication_node(input_path, project_node_path, output_path) 
    sample_node_path = os.path.join(output_path, 'sample_node.csv')
    publication_node_path = os.path.join(output_path, 'publication_node.csv')

    generate_mapping_relationships(experiment_node_path, sample_node_path, project_node_path, publication_node_path, output_path)
    # Node cleaning

    print(f"All nodes and mappings generated successfully in {output_path}.")
    
