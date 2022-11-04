import pandas as pd

apps = pd.read_csv("../../data_raw/abstract_matches_2mil_ALL.csv", usecols=["APPLICATION_ID"])
apps = apps.drop_duplicates()
apps = apps.sort_values(by=["APPLICATION_ID"])

apps.to_csv("../../data_neo4j/NormMap_mapped_app_ids.csv", index=None)


