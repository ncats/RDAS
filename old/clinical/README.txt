Description of each file and file dependencies. Important files start with **

# get list of rare diseases
  update_conditions_list.py:    webscrapes the clinical trials website to get the name of each rare disease. save list of rare diseases in conditions_list.txt
  extras/match_gard.py:         match conditions in conditions_list.txt to gard conditions using GARDId.csv. matches saved in conditions_matched.csv
  **conditions_matched.csv:     result of above code with added manual matches      

# create data model
  extras/data_model.csv:        data model as csv format: classes and fields chosen from user stories
  extras/read_data_model.py:    reads data_model.csv and prints python code that defines lists named after classes contatining fields for those classes
  **data_model.py:              this is the data model that the main code uses
  
# load neo4j
  **load_neo4j_functions.py:    list of helper functions. uses data_model.py
  **load_neo4j:                 loads neo4j. uses data_model.py
  
# make figures
  phase_trend.py:               makes png figure of current phase of trial by start date
  count_phase.py:               count the total number of trials in each phase
  
# others
  conditions_matched_short.csv: shortened version of conditions_matched.csv for testing
