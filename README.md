![Logo](https://rdas.ncats.nih.gov/assets/rdas_final_gradient_no_logo_small.webp)


The Rare Disease Alert System (RDAS) is a system designed for annotating and managing information related to rare diseases. Users may subscribe to specific rare diseases and get email alerts when new information becomes available.


## Code Repo Access

Not only will you need access to the RDAS repo to fully pull all the code but you will also need access to the Natural History Study repo and the Epidemiology repo we have

https://github.com/ncats/NaturalHistory4GARD/

https://github.com/ncats/epi4GARD/

## Setting up SSH keys Between The servers
Keep in mind that because we want to be as hands off in the fully running pipeline as possible, that there are some linux server setting you have to setup to make this possible, such as copying your SSH keys in the development server with the Testing server so that for example, we can transfer database files between them without having to input a password

`ssh-copy-id {host_name}`

Do this for the following:
Development SSH -> Testing server
Testing SSH -> Production server

## Bypassing sudo on Automation scripts
You may have to edit the Sudoers file in each server to bypass password prompts on the main pipeline scripts

`sudo visudo`

At the very bottom of the file I added the following:

`leadmandj ALL=(ALL) NOPASSWD:/home/leadmandj/RDAS/start_test.py`
**This one will slightly change depending on the server. Example: On production this will be start_prod.py**

`leadmandj ALL=(ALL) NOPASSWD:/opt/neo4j/bin/*`

`leadmandj ALL=(ALL) NOPASSWD:/usr/bin/chmod 777 /home/leadmandj/RDAS/*`


## Conda Environment
All the packages that are used within the scripts are installed within a conda environment, to create one locally:

`conda create -n rdas python=3.8.16`

then to activate the environment

`conda activate rdas` or `source activate rdas` (sometimes using conda doesnt work on some servers)

Then from here you can start setting environment variables by using:

`conda env config vars set VAR_NAME=VAR_VALUE`

Once variables are set you need to reset the environment for them to take effect:

`conda activate base` then `conda activate rdas` again

## Environment Variables

To run this project, you will need to add the following environment variables:

`AWS_ACCESS_KEY_ID`
`AWS_SECRET_ACCESS_KEY`
`PALANTIR_KEY`
`NEO4J_URI`
`NEO4J_USERNAME`
`NEO4J_PASSWORD`
`OMIM_KEY`
`AWS_PROFILE`
`NCBI_KEY` 

**AWS_ACCESS_KEY_ID**, **AWS_SECRET_ACCESS_KEY** = Amazon AWS User Credentials (For email service)

CONTACT - Carlin Biyoo about this

**AWS_PROFILE** = AWS profile to be used for services (set to "default")

**PALANTIR_KEY** = Palantir API key used for retrieving GARD related information from the DataLake project

CONTACT - Qian Zhu about this

**NEO4J_URI**, **NEO4J_USERNAME**, **NEO4J_PASSWORD** = Neo4j Database Credentials (For Python driver)
**NOTE** These credentials will be different depending on what server you are deploying code on to direct to the respective database

CONTACT - Carlin Biyoo about this

**OMIM_KEY** = OMIM API key used to populate the PubMed article database with OMIM information

LINK - https://www.omim.org/api

**NCBI_KEY** = NCBI Entrez Eutilz API key, used to populate PubMed article database with articles

LINK - https://account.ncbi.nlm.nih.gov/settings/

## Dependencies
You will also need the firebase database json key for the email scripts to work. The key will be stored in the base `crt` directory

There is a requirements.txt file in the github you can use to download many of the packages but there may be a few missing as it has not been updated in a bit
### Any remaining packages that need to be installed must be installed while the conda environment is activated and using the pip3 install command
# Quick Start
The RDAS system is currently not contained within a Docker container, right now it is more of a manual process of several scripts running simultaneously on different servers. For the full pipeline to function there will be a total of up to 6 scripts running on 3 different servers. Below is the current structure of which scripts run on what servers are used

| URL | Server Type | Usage |
| ----------- | ----------- | ----------- |
| [1] rdas-dev.ncats.nih.gov | Development | Active development of new features and for new updates to the database |
| [2] ncats-neo4j-lnx-dev.ncats.nih.gov | Development | For manually loading or generating databases or backups|
| [3] ncats-neo4j-lnx-test1.ncats.nih.gov | Testing | For manually loading or generating databases or backups |
| [4] ncats-neo4j-lnx-prod1.ncats.nih.gov | Production | For manually loading or generating databases or backups |

## DO THIS BEFORE RUNNING ANY SCRIPTS ON ANY SERVER
On the root of the RDAS directory, access `sysvars.py`. This file stores constants that are used in all of the scripts. Most of this file will be left unchanged but there are 6 fields that may be changed which are the fields:
- current_user
- base_directory_name
- db_prefix
- ct_db_name (aka Clinical Trial database name)
- pm_db_name (aka PubMed database name)
- gnt_db_name (aka Grants database name)
- gard_db_name (aka GARD database name)

`current_user` will be filled with your username on the linux system

`base_directory_name` will more than likely just stay as "RDAS" unless you clone the directory into another folder thats not on the root, for example if i clone RDAS into a folder named PRODUCTION on the root then this value will be "PRODUCTION/RDAS"

`db_prefix` this prefix will be applied to all databases when they are called upon in the code. For example adding "test." will add that prefix to all database names so that all the code queries the "test.{db_name}" databases. On the development server the prefix will likely be "test." while on the other 2 servers the prefix will be blank ""

`{type}_db_name` these will likely not change unless you want to change the name of the database you point to. Defaults are "rdas.ctkg", "rdas.pakg", "rdas.gfkg", and "rdas.gard"

### Development Server

#### How to run the below scripts in the background
Run the command
`nohup python3 {script_name} > logs/{name_of_output_file}.out &`

This will start the script in the background and redirect all print statements/output to the .out file in the logs folder. The name of the output file can be anything you want it to be

---

Up to 4 different scripts will be ran here, the update pipeline are seperate and if the entire pipeline is running updates for all of the databases then all 4 of these will be ran in the background simultaneously:

`start_ctkg_update.py` = runs constant update checks for the Clinical Trial database, triggers the update, and transfers the updated database to the Testing server

`start_pakg_update.py` = runs constant update checks for the PubMed database, triggers the update, and transfers the updated database to the Testing server

`start_gfkg_update.py` = runs constant update checks for the NIH Funded Projects (Grants) database, triggers the update, and transfers the updated database to the Testing server

`start_gard_update.py` = runs constant update checks for the GARD database, triggers the update, and transfers the updated database to the Testing server


### Testing Server

`start_test.py` = Checks the Testing server for incoming database files and replaces the current database on the Testing Neo4j with the transfered file. It also checks for manual approval of the Testing database to be transfered to Production


### Production Server

`start_prod.py` = Checks the Production server for an approved incoming database file and replaces the current database with that new database. The script then calculates the updates for each user of RDAS and sends email updates to them


### How To Run Each of The Main scripts
Run the scripts in the following order to start up the complete pipeline

1. PRODUCTION - `nohup python3 start_prod.py > logs/start_prod.out &`

2. TESTING - `nohup python3 start_test.py > logs/start_test.out &`

3. DEVELOPMENT - `nohup python3 start_ctkg_update.py > logs/start_ctkg_update.out &`

4. DEVELOPMENT - `nohup python3 start_pakg_update.py > logs/start_pakg_update.out &`

5. DEVELOPMENT - `nohup python3 start_gfkg_update.py > logs/start_gfkg_update.out &`

6. DEVELOPMENT - `nohup python3 start_gard_update.py > logs/start_gard_update.out &`




## Folder Structure
Any folder not mentioned in this section can be ignored

`backup` storage of previous files of database versions in case you need to revert changes

`crt` stores credentials, only thing in here right now is the firebase access key

`emails` store some of the scripts/files associated with generating the emails such as the template HTML for our user emails

`RDAS_CTKG` Contains the code that activates the database update for the Clinical Trial database and other associated methods

`RDAS_PAKG` Contains the code that activates the database update for the PubMed database and other associated methods

`RDAS_GFKG` Contains the code that activates the database update for the NIH Funded Project database and other associated methods

`RDAS_GARD` Contains the code that activates the database update for the GARD database and other associated methods

`RDAS_FAERS` Scripts for generating FAERS related data - Ask Jaber for more information [Not currently used in DB]

`RDAS_MEMGRAPH_APP` Scripts i recently started preparing for ease of use for when we switch to using Memgraph. I converted some previous code into objects for easier use in other scripts (such as the update scripts). This folder contains objects that send emails, generate dump (database) files, generates mapping files, transfers files to other servers, and triggers and checks for individual updates

`RDAS_Social_Network` - Scripts related to generating RDAS's social network information - Ask Jaber for more information [Not currently used in DB]

`RDAS_Target_Deconvolution` Also related to the Social Network code - Ask Jaber [Not currently used in DB]

`approved` This will only really be used on the TEST server, manually approved database files will be sent here and checked while running start_test.py. New files here will be sent to the PROD server

`transfer` This folder is where database files will go that are staged to be transfered to another server. This is the folder that the automated scripts check to see if there is a new database file on the server
## Feedback
If you have any feedback or questions about the RDAS website and using it, please reach out to us at `ncatsrdas@mail.nih.gov`


## Support
For those working on the RDAS project's codebase, please email Devon Leadman at shanmardev@hotmail.com for any questions on the code
