# Init.py
This script is responsible for initiating the creation and updates of each of the databases
This script will constantly run until the user kills the process
While it runs it will initiate an update every interval of time set by the user
If any of the databases are empty when first ran, it will create the empty databases from scratch

# Config.ini
Important information that can be imported into files using the configparser library
This includes credentials and when each of the databases have last been updated

# AlertCypher.py
This class allows you to instantiate an object for each individual database on the server and allows you to run queries specifically addressed to that individual database

# Clinical/generate.py, Grant/generate.py, Pubmed/generate.py
These scripts divert the programs path to either create the individual database from scratch or update it from the last update date
Scripts for creating and updated will be imported directly into this file to decrease clutter (See Clinical/generate.py create function)