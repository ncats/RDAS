
![Logo](https://rdas.ncats.nih.gov/assets/rdas_final_gradient.webp)


The Rare Disease Alert System (RDAS) is a system designed for annotating and managing information related to rare diseases. Users may subscribe to specific rare diseases and get email alerts when new information becomes available.


## Environment Variables

To run this project, you will need to add the following environment variables:

`AWS_ACCESS_KEY_ID`
`AWS_SECRET_ACCESS_KEY`
`PALANTIR_KEY`
`NEO4J_URI`
`NEO4J_USERNAME`
`NEO4J_PASSWORD`
`OMIM_KEY`
`METAMAP_EMAIL`
`METAMAP_KEY`
`AWS_PROFILE`
`NCBI_KEY` 

**AWS_ACCESS_KEY_ID**, **AWS_SECRET_ACCESS_KEY** = Amazon AWS User Credentials (For email service)

**AWS_PROFILE** = AWS profile to be used for services (set to "default")

**PALANTIR_KEY** = Palantir API key used for retrieving GARD related information from the DataLake project

**NEO4J_URI**, **NEO4J_USERNAME**, **NEO4J_PASSWORD** = Neo4j Database Credentials (For Python driver)

**OMIM_KEY** = OMIM API key used to populate the PubMed article database with OMIM information

**METAMAP_EMAIL**, **METAMAP_KEY** = MetaMap API key used to send information to their batch MetaMap services

**NCBI_KEY** = NCBI Entrez Eutilz API key, used to populate PubMed article database with articles
## Deployment
### Do the following for all 3 required servers (dev, test, prod)
* Create a conda environment with `Python v3.8.16`
* Pull the GitHub Repo to a local directory
* Inside of sysvars.py, change the `current_user` variable to your linux server username
* Download all of the dependencies from `requirements.txt` (preferable to install these dependencies within a Conda environment)
* Make sure the `chromedriver` binary is installed in your environment. Located in `RDAS/clinical/src/`
### Production Server
* Once the preceding steps have been completed, go to the root folder of the project ("RDAS") and run `nohup python3 -u start_prod.py > {name_out_output_file}.txt &`. This will run the start of the pipeline in the background as a process and send its output to the txt file.

### Testing Server
* Go to the root folder of the project ("RDAS") and run `nohup python3 -u start_test.py > {name_out_output_file}.txt &`

### Development Server
* Before you start the pipeline, check config.ini to make sure the last updated dates for the databases are correct
* Go to the root folder of the project ("RDAS") and run `nohup python3 -u start_dev.py > {name_out_output_file}.txt &`
## Feedback

If you have any feedback or questions, please reach out to us at `ncatsrdas@mail.nih.gov`

