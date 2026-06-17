import os
import pandas as pd
import json
import pickle
from neo4j import GraphDatabase
import traceback
import ast

def process_import(csv_path, json_path):
    """
    Main processing function to transform CSV to JSON and import data into Neo4j.
    """
    neo4j_uri = os.environ.get("NEO4J_URI")
    neo4j_user = os.environ.get("NEO4J_USER")
    neo4j_password = os.environ.get("NEO4J_PASSWORD")
    if not all([neo4j_uri, neo4j_user, neo4j_password]):
        raise ValueError("Set NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD before running this step.")

    try:
        # Step 1: Transform all CSV files to JSON
        #to_json(csv_path, json_path)

        # Step 2: Import data into Neo4j
        import_to_memgraph(json_path, neo4j_uri, neo4j_user, neo4j_password)

    except Exception as e:
        print(f"Error in process_import: {e}")
        raise

SCHEMA = {
    "publication_node.csv": "Pubmed_id",
    "project_node.csv": "Project_id",
    "experiment_node.csv": "Experiment_id",
    "sample_node.csv": "Sample_id",
    "platform_node.csv": "Platform_id",
    "condition_node.csv": "GardId"
}

def to_json(csv_path, json_path):
    """
    Convert all CSV files under 'csv_path' to JSON and save them in 'json_path'.
    Handle node files and mapping files appropriately.
    """
    try:
        os.makedirs(json_path, exist_ok=True)

        for file_name in os.listdir(csv_path):
            if file_name.endswith('.csv'):
                csv_file = os.path.join(csv_path, file_name)
                json_file = os.path.join(json_path, file_name.replace('.csv', '.json'))

                # Check if the file is a mapping file
                if "mapping" in file_name:
                    # Transform mapping file to include 'start' and 'end'  
                    # Mapping files don't require primary keys
                    df = pd.read_csv(csv_file)
                    mapping_data = []

                    if "sample_platform_mapping" in file_name:
                        for _, row in df.iterrows():
                            sample_id = row[0]  # First column is Sample_id
                            platform_ids = row[1]  # Second column is Platform_id(s)
                            
                            # Parse the platform_ids if it's a string representation of a list
                            if isinstance(platform_ids, str) and platform_ids.startswith('[') and platform_ids.endswith(']'):
                                try:
                                    platform_ids = ast.literal_eval(platform_ids)
                                except:
                                    platform_ids = [platform_ids]
                            elif not isinstance(platform_ids, list):
                                platform_ids = [platform_ids]
                            
                            # Create a mapping entry for each platform ID
                            for platform_id in platform_ids:
                                mapping_data.append({
                                    "start": {"label": "Sample", "key": "Sample_id", "value": sample_id},
                                    "end": {"label": "Platform", "key": "Platform_id", "value": platform_id}
                                })
                    elif "condition_project_mapping" in file_name:
                        for _, row in df.iterrows():
                            mapping_data.append({
                                "start": {"label": "Condition", "key": "GardId", "value": row["GardId"] },
                                "end": { "label": "Project", "key": "Project_id", "value": row["Project_id"] }
                            })
                    elif "publication_project_mapping" in file_name:
                        for _, row in df.iterrows():
                            mapping_data.append({
                                "start": {"label": "Publication", "key":"Pubmed_id", "value": row["Pubmed_id"]},
                                "end": {"label": "Project", "key": "Project_id", "value": row["Project_id"]}
                            })
                    elif "project_experiment_mapping" in file_name:
                        for _, row in df.iterrows():
                            mapping_data.append({
                                "start": {"label": "Project", "key": "Project_id", "value": row["Project_id"]},
                                "end": {"label": "Experiment", "key": "Experiment_id", "value": row["Experiment_id"]}
                            })
                    elif "experiment_platform_mapping" in file_name:
                        for _, row in df.iterrows():
                            mapping_data.append({
                                "start": {"label": "Experiment", "key": "Experiment_id", "value": row["Experiment_id"]},
                                "end": {"label": "Platform", "key": "Platform_id", "value": row["Platform_id"]}
                            })
                    elif "sample_experiment_mapping" in file_name:
                        for _, row in df.iterrows():
                            mapping_data.append({
                                "start": {"label": "Sample", "key": "Sample_id", "value": row["Sample_id"]},
                                "end": {"label": "Experiment", "key": "Experiment_id", "value": row["Experiment_id"]},
                            })
                    else:
                        # Generic handling for other mapping files
                        for _, row in df.iterrows():
                            # Assume first column is start and second is end
                            start_id = row.iloc[0]
                            end_id = row.iloc[1]
                            
                            # Determine labels and keys from filename
                            parts = file_name.replace('_mapping.csv', '').split('_')
                            if len(parts) >= 2:
                                start_label = parts[0].capitalize()
                                end_label = parts[1].capitalize()
                                
                                # Determine keys based on labels
                                start_key = f"{start_label}_id"
                                end_key = f"{end_label}_id"
                                
                                mapping_data.append({
                                    "start": {"label": start_label, "key": start_key, "value": start_id},
                                    "end": {"label": end_label, "key": end_key, "value": end_id}
                                })

                    with open(json_file, "w") as f:
                        json.dump(mapping_data, f, indent=4)

                else:
                    # Node files require primary keys
                    primary_key = SCHEMA.get(file_name)
                    if primary_key is None:
                        raise ValueError(f"Primary key not defined for {file_name}")

                    # Read CSV
                    df = pd.read_csv(csv_file)

                    # Ensure primary key exists
                    if primary_key not in df.columns:
                        raise ValueError(f"Primary key '{primary_key}' not found in {file_name}")

                    # Convert to JSON
                    df.to_json(json_file, orient='records', indent=4)

                print(f"Converted {file_name} to JSON at {json_file}")

    except Exception as e:
        print(f"Error in to_json: {e}")
        raise

def create_node(session, file_path, batch_size=500, checkpoint_file=None):
    """
    Import nodes from a JSON file into Memgraph using batch transactions with checkpointing.
    """
    try:
        # Load the JSON file
        with open(file_path, 'r') as f:
            nodes = json.load(f)

        # Determine the label for the nodes based on the file name
        label = os.path.basename(file_path).replace('_node.json', '').capitalize()
        print(f"  -> Importing nodes with label: {label} from {file_path}")
        
        # Initialize checkpoint data
        start_index = 0
        checkpoint_path = checkpoint_file or f"{file_path}.checkpoint"
        
        # Check if checkpoint exists and load it
        if os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path, 'rb') as cp:
                    checkpoint_data = pickle.load(cp)
                    start_index = checkpoint_data.get('last_processed_index', 0) + 1
                    print(f"  -> Resuming from checkpoint at index {start_index}")
            except Exception as e:
                print(f"  -> Error loading checkpoint, starting from beginning: {e}")
                start_index = 0

        # Process nodes in batches
        total_nodes = len(nodes)
        for i in range(start_index, total_nodes, batch_size):
            # Create a batch of nodes
            batch = nodes[i:min(i+batch_size, total_nodes)]
            
            # Use an explicit transaction for the batch
            with session.begin_transaction() as tx:
                for node in batch:
                    cypher_query = f"""
                    CREATE (n:{label})
                    SET n = $properties
                    """
                    tx.run(cypher_query, properties=node)
                
                # Commit the transaction for this batch
                tx.commit()
            
            # Save checkpoint after each batch
            with open(checkpoint_path, 'wb') as cp:
                pickle.dump({'last_processed_index': min(i+batch_size, total_nodes)-1}, cp)
            
            # Log progress
            print(f"    -> Imported {min(i+batch_size, total_nodes)}/{total_nodes} nodes for label {label}")
        
        # Remove checkpoint file when done
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
            
        print(f"  -> Finished importing nodes for label {label}")

    except Exception as e:
        print(f"Error in create_node for {file_path}: {e}")
        traceback.print_exc()    
        raise

def create_relationships(session, file_path, batch_size=500, checkpoint_file=None):
    """
    Import relationships from a JSON file into Memgraph using batch transactions with checkpointing.
    """
    try:
        # Load the JSON file
        with open(file_path, 'r') as f:
            relationships = json.load(f)

        # Determine the relationship type based on the file name
        relationship_type = os.path.basename(file_path).replace('_mapping.json', '').upper()
        print(f"  -> Importing relationships of type: {relationship_type} from {file_path}")
        
        # Initialize checkpoint data
        start_index = 0
        checkpoint_path = checkpoint_file or f"{file_path}.checkpoint"
        
        # Check if checkpoint exists and load it
        if os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path, 'rb') as cp:
                    checkpoint_data = pickle.load(cp)
                    start_index = checkpoint_data.get('last_processed_index', 0) + 1
                    print(f"  -> Resuming from checkpoint at index {start_index}")
            except Exception as e:
                print(f"  -> Error loading checkpoint, starting from beginning: {e}")
                start_index = 0

        # Verify current count in database
        with session.begin_transaction() as tx:
            count_query = f"""
            MATCH ()-[r:{relationship_type}]->() 
            RETURN count(r) as rel_count
            """
            result = tx.run(count_query)
            current_count = result.single()["rel_count"]
            print(f"  -> Current count of {relationship_type} relationships in database: {current_count}")

        # Process relationships in batches
        total_rels = len(relationships)
        successful_imports = 0
        
        for i in range(start_index, total_rels, batch_size):
            # Create a batch of relationships
            batch = relationships[i:min(i+batch_size, total_rels)]
            batch_success = 0
            
            # Use an explicit transaction for the batch
            with session.begin_transaction() as tx:
                for rel in batch:
                    # Validate the structure of each relationship
                    if not rel.get('start') or not rel.get('end'):
                        print(f"  -> Skipping invalid relationship in {file_path}: {rel}")
                        continue

                    # Extract details for the relationship
                    start_label = rel['start']['label']
                    start_key = rel['start']['key']
                    start_value = rel['start']['value']

                    end_label = rel['end']['label']
                    end_key = rel['end']['key']
                    end_value = rel['end']['value']
                    
                    # Handle case where value is a list - extract the first element
                    if isinstance(start_value, list) and len(start_value) > 0:
                        start_value = start_value[0]
                    if isinstance(end_value, list) and len(end_value) > 0:
                        end_value = end_value[0]
                    
                    # Handle stringified list case
                    if isinstance(start_value, str) and start_value.startswith('[') and start_value.endswith(']'):
                        try:
                            start_value = ast.literal_eval(start_value)[0]
                        except:
                            # Keep as is if parsing fails
                            pass

                    if isinstance(end_value, str) and end_value.startswith('[') and end_value.endswith(']'):
                        try:
                            end_value = ast.literal_eval(end_value)[0]
                        except:
                            # Keep as is if parsing fails
                            pass

                    # Safety check to avoid inserting None accidentally
                    if start_value is None or end_value is None:
                        print(f"  -> Skipping relationship due to missing start or end value: Start={start_value}, End={end_value}")
                        continue

                    cypher_query = f"""
                    MATCH (a:{start_label} {{ {start_key}: $start_value }}),
                          (b:{end_label} {{ {end_key}: $end_value }})
                    MERGE (a)-[r:{relationship_type}]->(b)
                    RETURN count(r) as rel_count
                    """
                    result = tx.run(
                        cypher_query,
                        start_value=start_value,
                        end_value=end_value
                    )
                    
                    # Check if relationship was created or already existed
                    summary = result.consume()
                    if summary.counters.relationships_created > 0:
                        batch_success += 1
                    elif summary.counters.relationships_created == 0:
                        # Check if nodes exist
                        check_query = f"""
                        MATCH (a:{start_label} {{ {start_key}: $start_value }})
                        RETURN count(a) as start_count
                        """
                        start_result = tx.run(check_query, start_value=start_value)
                        start_count = start_result.single()["start_count"]
                        
                        check_query = f"""
                        MATCH (b:{end_label} {{ {end_key}: $end_value }})
                        RETURN count(b) as end_count
                        """
                        end_result = tx.run(check_query, end_value=end_value)
                        end_count = end_result.single()["end_count"]
                        
                        if start_count == 0 or end_count == 0:
                            print(f"    -> Warning: Could not create relationship - Start node exists: {start_count > 0}, End node exists: {end_count > 0}")
                            print(f"       Start: {start_label}({start_key}={start_value}), End: {end_label}({end_key}={end_value})")
                            
                            # If end node doesn't exist but should, print more details
                            if end_count == 0:
                                print(f"       Original end value in mapping: {rel['end']['value']}")
                                # Try to find similar nodes
                                similar_query = f"""
                                MATCH (b:{end_label})
                                RETURN b.{end_key} as id LIMIT 5
                                """
                                similar_result = tx.run(similar_query)
                                similar_ids = [record["id"] for record in similar_result]
                                print(f"       Sample of existing {end_label} IDs: {similar_ids}")
                
                # Commit the transaction for this batch
                tx.commit()
                successful_imports += batch_success
            
            # Save checkpoint after each batch
            with open(checkpoint_path, 'wb') as cp:
                pickle.dump({'last_processed_index': min(i+batch_size, total_rels)-1}, cp)
            
            # Log progress with actual success count
            print(f"    -> Processed {min(i+batch_size, total_rels)}/{total_rels} relationships, successfully imported {successful_imports} relationships of type {relationship_type}")
            
            # Verify count in database periodically
            if i % (batch_size * 10) == 0:
                with session.begin_transaction() as tx:
                    count_query = f"""
                    MATCH ()-[r:{relationship_type}]->() 
                    RETURN count(r) as rel_count
                    """
                    result = tx.run(count_query)
                    current_count = result.single()["rel_count"]
                    print(f"    -> Current count of {relationship_type} relationships in database: {current_count}")
        
        # Final count verification
        with session.begin_transaction() as tx:
            count_query = f"""
            MATCH ()-[r:{relationship_type}]->() 
            RETURN count(r) as rel_count
            """
            result = tx.run(count_query)
            final_count = result.single()["rel_count"]
            print(f"  -> Final count of {relationship_type} relationships in database: {final_count}")
            print(f"  -> Successfully imported {successful_imports} relationships of type {relationship_type}")
        
        # Remove checkpoint file when done
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
            
        print(f"  -> Finished importing relationships of type {relationship_type}")

    except Exception as e:
        print(f"Error in create_relationships for {file_path}: {e}")
        traceback.print_exc()
        raise

def import_to_memgraph(json_path, uri, user, password, batch_size=500):
    """
    Connect to Memgraph and import nodes and relationships from JSON files with checkpointing.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    # Create checkpoint directory
    checkpoint_dir = os.path.join(json_path, "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Track overall progress
    progress_file = os.path.join(checkpoint_dir, "import_progress.pkl")
    processed_files = set()
    
    # Load progress if exists
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'rb') as f:
                processed_files = pickle.load(f)
            print(f"Resuming import. {len(processed_files)} files already processed.")
        except Exception as e:
            print(f"Error loading progress file, starting from beginning: {e}")
            processed_files = set()

    try:
        with driver.session() as session:
            """
            # Step 1: Import all nodes
            print("Step 1: Importing all nodes...")
            node_files = [f for f in os.listdir(json_path) if f.endswith('.json') and "mapping" not in f]
            for file_name in node_files:
                if file_name in processed_files:
                    print(f"  -> Skipping already processed file: {file_name}")
                    continue
                    
                file_path = os.path.join(json_path, file_name)
                checkpoint_path = os.path.join(checkpoint_dir, f"{file_name}.checkpoint")
                print(f"  -> Processing node file: {file_name}")
                create_node(session, file_path, batch_size, checkpoint_path)
                
                # Mark file as processed
                processed_files.add(file_name)
                with open(progress_file, 'wb') as f:
                    pickle.dump(processed_files, f)
            """
            # Step 2: Import all relationships
            print("Step 2: Importing all relationships...")
            rel_files = [f for f in os.listdir(json_path) if f.endswith('.json') and "mapping" in f]
            for file_name in rel_files:
                if file_name in processed_files:
                    print(f"  -> Skipping already processed file: {file_name}")
                    continue
                    
                file_path = os.path.join(json_path, file_name)
                checkpoint_path = os.path.join(checkpoint_dir, f"{file_name}.checkpoint")
                print(f"  -> Processing relationship file: {file_name}")
                create_relationships(session, file_path, batch_size, checkpoint_path)
                
                # Mark file as processed
                processed_files.add(file_name)
                with open(progress_file, 'wb') as f:
                    pickle.dump(processed_files, f)

        # Clean up checkpoint directory when everything is done
        if os.path.exists(checkpoint_dir):
            import shutil
            shutil.rmtree(checkpoint_dir)
            print("Import completed successfully. Checkpoint files removed.")

    except Exception as e:
        print(f"Error in import_to_memgraph: {e}")
        print("Checkpoint files preserved for resuming import later.")
        traceback.print_exc()
        raise

    finally:
        driver.close()





