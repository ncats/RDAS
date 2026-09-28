"""Verify that the two directional HPO gene/phenotype tables agree."""

import sys
from pathlib import Path
from typing import Iterator, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


BATCH_SIZE = 10_000
EXAMPLE_LIMIT = 10
PROGRESS_INTERVAL = 100_000
MAPPING_COLUMNS = (
    "ncbi_gene_id",
    "gene_symbol",
    "hpo_id",
    "hpo_name",
    "disease_id",
)
GENE_TO_PHENOTYPE_SQL = """
    SELECT ncbi_gene_id, gene_symbol, hpo_id, hpo_name, disease_id
    FROM rdas_db.extension_hpo_genes_to_phenotype
    ORDER BY
        BINARY ncbi_gene_id,
        BINARY gene_symbol,
        BINARY hpo_id,
        BINARY hpo_name,
        BINARY disease_id
"""
PHENOTYPE_TO_GENE_SQL = """
    SELECT ncbi_gene_id, gene_symbol, hpo_id, hpo_name, disease_id
    FROM rdas_db.extension_hpo_phenotype_to_genes
    ORDER BY
        BINARY ncbi_gene_id,
        BINARY gene_symbol,
        BINARY hpo_id,
        BINARY hpo_name,
        BINARY disease_id
"""


def iter_rows(cursor, sql: str, table_name: str) -> Iterator[Tuple[str, ...]]:

    """Stream one table in exact binary order without loading it into memory."""

    print(f"Executing sorted query for {table_name}...", flush=True)
    cursor.execute(sql)
    print(f"Sorted query for {table_name} is ready.", flush=True)
    fetched_rows = 0

    while True:
        batch = cursor.fetchmany(BATCH_SIZE)

        if not batch:
            break

        fetched_rows += len(batch)
        print(f"{table_name}: fetched rows={fetched_rows}.", flush=True)

        for row in batch:
            yield tuple("" if value is None else str(value) for value in row)


def iter_grouped_rows(rows: Iterator[Tuple[str, ...]]) -> Iterator[Tuple[Tuple[str, ...], int]]:

    """Collapse adjacent identical mappings and return each mapping with its count."""

    current_row = next(rows, None)

    while current_row is not None:
        row_count = 1
        next_row = next(rows, None)

        while next_row == current_row:
            row_count += 1
            next_row = next(rows, None)

        yield current_row, row_count
        current_row = next_row


def mapping_example(mapping: Tuple[str, ...], row_count: int) -> str:

    values = ", ".join(f"{column}={value!r}" for column, value in zip(MAPPING_COLUMNS, mapping))
    return f"{values}, row_count={row_count}"


def main() -> None:

    print("Connecting to MySQL for read-only mapping verification...", flush=True)
    connections = DBConnection()
    gene_to_phenotype_mysql = connections.mysql_conn()
    phenotype_to_gene_mysql = connections.mysql_conn()

    if gene_to_phenotype_mysql is None or phenotype_to_gene_mysql is None:
        if gene_to_phenotype_mysql is not None and gene_to_phenotype_mysql.is_connected():
            gene_to_phenotype_mysql.close()

        if phenotype_to_gene_mysql is not None and phenotype_to_gene_mysql.is_connected():
            phenotype_to_gene_mysql.close()

        raise ConnectionError("Unable to open both MySQL connections.")

    print("Connected to MySQL.", flush=True)
    gene_to_phenotype_cursor = gene_to_phenotype_mysql.cursor()
    phenotype_to_gene_cursor = phenotype_to_gene_mysql.cursor()

    try:
        gene_rows = iter_grouped_rows(
            iter_rows(
                gene_to_phenotype_cursor,
                GENE_TO_PHENOTYPE_SQL,
                "extension_hpo_genes_to_phenotype",
            )
        )
        phenotype_rows = iter_grouped_rows(
            iter_rows(
                phenotype_to_gene_cursor,
                PHENOTYPE_TO_GENE_SQL,
                "extension_hpo_phenotype_to_genes",
            )
        )
        gene_group = next(gene_rows, None)
        phenotype_group = next(phenotype_rows, None)
        gene_total_rows = 0
        phenotype_total_rows = 0
        matched_distinct_mappings = 0
        only_in_gene_table = 0
        only_in_phenotype_table = 0
        gene_duplicate_rows = 0
        phenotype_duplicate_rows = 0
        multiplicity_mismatches = 0
        gene_only_examples = []
        phenotype_only_examples = []
        multiplicity_examples = []
        compared_distinct_mappings = 0
        next_progress = PROGRESS_INTERVAL

        """
        This is a merge comparison of two sorted streams. For example, the
        shared mapping below must appear with the same values and row count in
        both directions:
            ncbi_gene_id="16"
            gene_symbol="AARS1"
            hpo_id="HP:0002460"
            hpo_name="Distal muscle weakness"
            disease_id="OMIM:613287"
        """
        while gene_group is not None or phenotype_group is not None:
            if phenotype_group is None or (gene_group is not None and gene_group[0] < phenotype_group[0]):
                mapping, row_count = gene_group
                gene_total_rows += row_count
                gene_duplicate_rows += row_count - 1
                only_in_gene_table += 1

                if len(gene_only_examples) < EXAMPLE_LIMIT:
                    gene_only_examples.append(mapping_example(mapping, row_count))

                gene_group = next(gene_rows, None)

            elif gene_group is None or phenotype_group[0] < gene_group[0]:
                mapping, row_count = phenotype_group
                phenotype_total_rows += row_count
                phenotype_duplicate_rows += row_count - 1
                only_in_phenotype_table += 1

                if len(phenotype_only_examples) < EXAMPLE_LIMIT:
                    phenotype_only_examples.append(mapping_example(mapping, row_count))

                phenotype_group = next(phenotype_rows, None)

            else:
                mapping = gene_group[0]
                gene_row_count = gene_group[1]
                phenotype_row_count = phenotype_group[1]
                gene_total_rows += gene_row_count
                phenotype_total_rows += phenotype_row_count
                gene_duplicate_rows += gene_row_count - 1
                phenotype_duplicate_rows += phenotype_row_count - 1
                matched_distinct_mappings += 1

                if gene_row_count != phenotype_row_count:
                    multiplicity_mismatches += 1

                    if len(multiplicity_examples) < EXAMPLE_LIMIT:
                        multiplicity_examples.append(
                            f"{mapping_example(mapping, gene_row_count)}, "
                            f"phenotype_to_gene_row_count={phenotype_row_count}"
                        )

                gene_group = next(gene_rows, None)
                phenotype_group = next(phenotype_rows, None)

            compared_distinct_mappings += 1

            if compared_distinct_mappings >= next_progress:
                print(f"Compared distinct mappings={compared_distinct_mappings}...", flush=True)
                next_progress += PROGRESS_INTERVAL

        mapping_sets_match = only_in_gene_table == 0 and only_in_phenotype_table == 0
        row_counts_match = multiplicity_mismatches == 0
        verification_passed = mapping_sets_match and row_counts_match

        print("\nVERIFICATION SUMMARY")
        print(f"gene_to_phenotype_rows={gene_total_rows}")
        print(f"phenotype_to_gene_rows={phenotype_total_rows}")
        print(f"matched_distinct_mappings={matched_distinct_mappings}")
        print(f"only_in_gene_to_phenotype={only_in_gene_table}")
        print(f"only_in_phenotype_to_gene={only_in_phenotype_table}")
        print(f"gene_to_phenotype_duplicate_rows={gene_duplicate_rows}")
        print(f"phenotype_to_gene_duplicate_rows={phenotype_duplicate_rows}")
        print(f"mapping_multiplicity_mismatches={multiplicity_mismatches}")
        print(f"mapping_set_status={'PASS' if mapping_sets_match else 'FAIL'}")
        print(f"row_multiplicity_status={'PASS' if row_counts_match else 'FAIL'}")
        print(f"overall_status={'PASS' if verification_passed else 'FAIL'}")

        if gene_only_examples:
            print("\nExamples only in extension_hpo_genes_to_phenotype:")

            for example in gene_only_examples:
                print(f"  {example}")

        if phenotype_only_examples:
            print("\nExamples only in extension_hpo_phenotype_to_genes:")

            for example in phenotype_only_examples:
                print(f"  {example}")

        if multiplicity_examples:
            print("\nExamples with different row counts:")

            for example in multiplicity_examples:
                print(f"  {example}")

        if not verification_passed:
            raise RuntimeError("Gene/Phenotype mapping verification failed; review the summary and examples above.")

    finally:
        gene_to_phenotype_cursor.close()
        phenotype_to_gene_cursor.close()

        if gene_to_phenotype_mysql.is_connected():
            gene_to_phenotype_mysql.close()

        if phenotype_to_gene_mysql.is_connected():
            phenotype_to_gene_mysql.close()


# conda run --no-capture-output -n rdas python Y_extension/hpo/20260925/graph/step_3_1_verify_gene_phenotype_mappings.py
if __name__ == "__main__":
    main()
