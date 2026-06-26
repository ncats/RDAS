import argparse
import logging
import os

os.makedirs("logs", exist_ok=True)
logging.basicConfig(filename=os.path.join("logs", "pipeline.log"), level=logging.INFO)

def _load_pipeline_modules():
    if __package__:
        from . import (
            step1_search_geo,
            step2_download_matrix_optional as step2_download_matrix,
            step3_download_gse_number,
            step4_extract_to_table,
            step5_generate_node_mappings,
            step6_import_to_neo4j,
            step7_experiment_normalization,
            step7_1_import_normalized_experiment_properties,
        )
        from .utils import load_paths
    else:
        import step1_search_geo
        import step2_download_matrix_optional as step2_download_matrix
        import step3_download_gse_number
        import step4_extract_to_table
        import step5_generate_node_mappings
        import step6_import_to_neo4j
        import step7_experiment_normalization
        import step7_1_import_normalized_experiment_properties
        from utils import load_paths

    return (
        step1_search_geo,
        step2_download_matrix,
        step3_download_gse_number,
        step4_extract_to_table,
        step5_generate_node_mappings,
        step6_import_to_neo4j,
        step7_experiment_normalization,
        step7_1_import_normalized_experiment_properties,
        load_paths,
    )


def main(args):
    (
        step1_search_geo,
        step2_download_matrix,
        step3_download_gse_number,
        step4_extract_to_table,
        step5_generate_node_mappings,
        step6_import_to_neo4j,
        step7_experiment_normalization,
        step7_1_import_normalized_experiment_properties,
        load_paths,
    ) = _load_pipeline_modules()
    paths = load_paths(args.config)
    
    try:
        logging.info("Starting GEO data pipeline")

        if args.step1_search_geo:
            step1_search_geo.process_disease_file(
                input_file=paths["disease_list_combined_file"],
                output_file=paths["disease_list_combined_with_count"],
                batch_size=args.batch_size,
            )
            logging.info("Step 1 completed: Series count updated.")
        
        if args.step2_download_matrix:
            step2_download_matrix.process_diseases_and_download_matrix(
                input_file=paths["disease_list_combined_with_count"],
                output_dir=paths["geo_matrix_files"],
            )
            logging.info("Step 2 completed: Matrix files downloaded.")

        if args.step3_download_gse_number:
            step3_download_gse_number.record_gse_number(
                input_file=paths["disease_list_combined_with_count"],
                output_file=paths["gse_ids_csv"],
            )
            logging.info("Step3 completed: GSE IDS recorded.")

        if args.step4_extract_to_table:
            step4_extract_to_table.extract_to_table(
                input_file=paths["gse_ids_csv"],
                table_template=paths["geo_table_template"],
                disease_name_list = paths["disease_list_combined_with_count"],
                output_dir=paths["geo_final_tables"]
            )
            logging.info("Step4 completed: Extracted to tables.")

        if args.step5_generate_node_mappings:
            step5_generate_node_mappings.process_all_nodes(
                input_path=paths["geo_final_tables"],
                output_path=paths["node_csv_files"]
            )

        if args.step6_import_to_neo4j:
            step6_import_to_neo4j.process_import(
                csv_path=paths["node_csv_files"],
                json_path=paths["node_json_files"]
            )

        if args.step7_experiment_normalization:
            step7_experiment_normalization.normalize_experiment_data(args.config)

        if args.step7_1_import_normalized_experiment_properties:
            step7_1_import_normalized_experiment_properties.import_normalized_properties(args.config)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

    logging.info("GEO Extration pipeline completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GEO Data Pipeline")
    parser.add_argument(
        "--step1-search-geo", action="store_true", help="Run Step1: Search GEO datasets with count."
    )
    parser.add_argument(
        "--step2-download-matrix", action="store_true", help="Run Step2: Download matrix files."
    )
    parser.add_argument(
        "--step3-download-gse-number", action="store_true", help="Run Step3: Record GSE numbers."
    )
    parser.add_argument(
        "--step4-extract-to-table", action="store_true", help="Run Step4: Extracted to tables."
    )
    parser.add_argument(
        "--step5-generate-node-mappings", action="store_true", help="Run Step5: Generate nodes and mapping relationships csv files."
    ) 
    parser.add_argument(
        "--step6-import-to-neo4j", action="store_true", help="Run Step6: Import nodes and mapping relationships to neo4j db."
    ) 
    parser.add_argument(
        "--step7-experiment-normalization", action="store_true", help="Run Step7: Normalize experiment node properties."
    )
    parser.add_argument(
        "--step7-1-import-normalized-experiment-properties",
        action="store_true",
        help="Run Step7.1: Import normalized experiment properties into the graph database."
    )
    
    # Also need to add the batch size as an argument in the parser
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size for processing.")
    parser.add_argument("--config", help="Optional path to a YAML file with input/output paths.")

    parser.add_argument("--all", action="store_true", help="Run all steps sequentially.")

    args = parser.parse_args()

    # If --all is specified, set all other arguments to True
    if args.all:
        #args.step1_search_geo = True
        #args.step2_download_matrix = True
        args.step3_download_gse_number = True
        args.step4_extract_to_table = True
        args.step5_generate_node_mappings = True
        args.step6_import_to_neo4j = True
        args.step7_experiment_normalization = True
        args.step7_1_import_normalized_experiment_properties = True
    main(args)
