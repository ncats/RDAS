import glob
import pandas as pd
from prepare_annotation_text import prepare_phr_aim

ENCODING = 'latin1'


# Get CSV files lists from projects and abstracts folders
projects_path = "../../data_neo4j/projects"
projects_files = glob.glob(projects_path + "/*.csv")

abstracts_path = "../../data_neo4j/abstracts"
abstracts_files = glob.glob(abstracts_path + "/*.csv")


for projects_file, abstracts_file in zip(projects_files, abstracts_files):

    annotate_text = prepare_phr_aim(projects_file, abstracts_file)
    
    yr_idx = len(projects_path) + 13
    year = projects_file[yr_idx:yr_idx+4]
    
    output_file = "../../data_neo4j/annotation_files/annotation_text_" + year + ".csv" 
    annotate_text.to_csv(output_file, index=False, encoding=ENCODING)

    print("Finished", output_file)


print("................. ALL DONE! .................")
