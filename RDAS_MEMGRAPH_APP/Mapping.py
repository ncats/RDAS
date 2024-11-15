import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append('/home/leadmandj/RDAS/')
sys.path.append(os.getcwd())
import sysvars
from AlertCypher import AlertCypher
from subprocess import *
from time import sleep
from datetime import datetime
import pandas as pd

class Mapping:
    def __init__ (self, mode=None):
        """
        Initialize the Mapping class with the specified mode.

        :param mode: The environment or database mode ('ct', 'pm', 'gnt', 'gard').
        :raises Exception: If the mode is not in sysvars.dump_dirs.
        """
        if mode in sysvars.dump_dirs:
            self.mode = mode
        else:
            raise Exception('Not in sysvars')
        
    def load_mapping_file (self, mode=None):
        """
        Load a mapping file into the specified database.

        :param mode: The database mode. Defaults to the class's mode attribute.
        :raises Exception: If the mode is invalid.
        """
        mode = self.mode
        if not mode: raise Exception ('Note a valid mode')

        db = AlertCypher(mode)
        print(f'Deleting {mode} previous connections after 10 seconds...')
        sleep(10)

        # Handle mode-specific operations
        if sysvars.db_abbrevs2[mode] == 'ct':
            print('Deleting previous connections... may take up to 30 minutes')
            db.run('MATCH (x:ClinicalTrial)-[r]-(y:GARD) WHERE NOT x.NCTId IS NULL CALL {WITH r DELETE r} IN TRANSACTIONS OF 10000 ROWS')
            print(f'Previous {mode} connections deleted, loading mapping file from {sysvars.ct_files_path}trial_to_gard_mappings.csv')

            db.run(f'''LOAD CSV WITH HEADERS FROM \"file:///{sysvars.ct_files_path}trial_to_gard_mappings.csv\" AS row
                        CALL {{
                        WITH row MATCH (x:GARD) WHERE x.GardId = row.GARD MATCH (y:ClinicalTrial) WHERE y.NCTid = row.NCTID MERGE (x)-[:mapped_to_gard {{MatchedTermRDAS: data.TERM}}]->(y)
                        }} IN TRANSACTIONS OF 10000 ROWS''')
                
        elif sysvars.db_abbrevs2[mode] == 'pm':
            print('Deleting previous connections... may take up to 30 minutes')
            db.run('MATCH (x:Article)-[r]-(y:GARD) WHERE NOT x.pubmed_id IS NULL CALL {WITH r DELETE r} IN TRANSACTIONS OF 10000 ROWS')
            print(f'Previous {mode} connections deleted, loading mapping file from {sysvars.pm_files_path}article_to_gard_mappings.csv...')

            db.run(f'''LOAD CSV WITH HEADERS FROM \"file:///{sysvars.pm_files_path}article_to_gard_mappings.csv\" AS row
                        CALL {{
                        WITH row MATCH (x:GARD) WHERE x.GardId = row.GARD MATCH (y:Article) WHERE y.pubmed_id = row.PMID MERGE (x)-[r:MENTIONED_IN]->(y) SET r.MatchedTermRDAS = row.TERM, r.ReferenceOrigin = row.MATCH_TYPE
                        }} IN TRANSACTIONS OF 10000 ROWS''')
                
        elif sysvars.db_abbrevs2[mode] == 'gnt':
            print('Deleting previous connections... may take up to 30 minutes')
            db.run('MATCH (x:Project)-[r]-(y:GARD) WHERE NOT x.application_id IS NULL CALL {WITH r DELETE r} IN TRANSACTIONS OF 10000 ROWS')
            print(f'Previous {mode} connections deleted, loading mapping file from {sysvars.gnt_files_path}project_to_gard_mappings.csv')

            db.run(f'''LOAD CSV WITH HEADERS FROM \"file:///{sysvars.gnt_files_path}project_to_gard_mappings.csv\" AS row
                        CALL {{
                        WITH row MATCH (x:GARD) WHERE x.GardId = row.GARD
                        MATCH (y:Project) WHERE y.application_id = toInteger(row.APPLICATION_ID)
                        MERGE (x)-[:RESEARCHED_BY]->(y)
                        }} IN TRANSACTIONS OF 10000 ROWS''')

        elif sysvars.db_abbrevs2[mode] == 'gard':
            print('Deleting previous connections... may take up to 30 minutes')
            db.run('MATCH (x:GARD)-[r]->(y:GARD) WHERE NOT x.GardId IS NULL CALL {WITH r DELETE r} IN TRANSACTIONS OF 10000 ROWS')
            print(f'Previous {mode} connections deleted, loading mapping file from {sysvars.gard_files_path}gard_to_gard_mappings.csv')

            db.run(f'''LOAD CSV WITH HEADERS FROM \"file:///{sysvars.gard_files_path}gard_to_gard_mappings.csv\" AS row
                        CALL {{
                        WITH row MATCH (x:GARD) WHERE x.GardId = row.CHILD_GARD MATCH (y:GARD) WHERE y.GardId = row.PARENT_GARD MERGE (x)-[:subClassOf]->(y)
                        }} IN TRANSACTIONS OF 10000 ROWS''')

    def generate_mapping_file (self, mode=None):
        """
        Generate mapping files by extracting data from the database.

        :param mode: The database mode. Defaults to the class's mode attribute.
        :raises Exception: If the mode is invalid.
        """
        mode = self.mode
        if not mode: raise Exception ('Note a valid mode')

        db = AlertCypher(mode)

        # Generate RDAS.CTKG Mapping File
        if sysvars.db_abbrevs2[mode] == 'ct':
            # Gather ClinicalTrial to GARD mappings
            all_mappings = list()
            columns = ['NCTID', 'GARD', 'TERM']
            response = db.run('MATCH (x:ClinicalTrial)-[r]-(y:GARD) WHERE NOT x.NCTId IS NULL RETURN x.NCTId as nctid, y.GardId as gardid, r.MatchedTermRDAS as term').data()
            response_length = len(response)-1
            print('Extracting Data...')
            for idx, row in enumerate(response):
                print(idx,'/',response_length)

                nctid = row['nctid']
                gardid = row['gardid']
                term = row['term']

                all_mappings.append([nctid, term, gardid])

            df = pd.DataFrame(all_mappings, columns=columns)
            df.to_csv(f'{sysvars.ct_files_path}trial_to_gard_mappings.csv', index=False)

        # Generate RDAS.PAKG Mapping File
        elif sysvars.db_abbrevs2[mode] == 'pm':
            # Gather Article to GARD mappings
            all_mappings = list()
            columns = ['PMID', 'GARD', 'TERM', 'MATCH_TYPE']
            response = db.run('MATCH (x:Article)-[r]-(y:GARD) WHERE NOT x.pubmed_id IS NULL RETURN x.pubmed_id as pmid, y.GardId as gardid, r.MatchedTermRDAS as term, r.ReferenceOrigin as origin').data()
            response_length = len(response)-1
            print('Extracting Data...')
            for idx, row in enumerate(response):
                print(idx,'/',response_length)

                pmid = row['pmid']
                gardid = row['gardid']
                origin = row['origin']
                if not origin: origin = 'PubMed-API'
                term = row['term']

                all_mappings.append([pmid, gardid, term, origin])

            df = pd.DataFrame(all_mappings, columns=columns)
            df.to_csv(f'{sysvars.pm_files_path}article_to_gard_mappings.csv', index=False)

        # Generate RDAS.GFKG Mapping File
        elif sysvars.db_abbrevs2[mode] == 'gnt':
            # Gather Project to GARD mappings
            all_mappings = list()
            columns = ['APPLICATION_ID', 'GARD']
            response = db.run('MATCH (x:Project)-[r]-(y:GARD) WHERE NOT x.application_id IS NULL RETURN x.application_id as appid, y.GardId as gardid').data()
            response_length = len(response)-1
            print('Extracting Data...')
            for idx, row in enumerate(response):
                print(idx,'/',response_length)

                appid = row['appid']
                gardid = row['gardid']

                all_mappings.append([appid, gardid])

            df = pd.DataFrame(all_mappings, columns=columns)
            df.to_csv(f'{sysvars.gnt_files_path}project_to_gard_mappings.csv', index=False)

        # Generate RDAS.GARD Mapping File
        elif sysvars.db_abbrevs2[mode] == 'gard':
            # Gather GARD to GARD mappings
            all_mappings = list()
            columns = ['PARENT_GARD', 'CHILD_GARD']
            response = db.run('MATCH (x:GARD)-[r]->(y:GARD) WHERE NOT x.GardId IS NULL RETURN x.GardId as childid, y.GardId as parentid').data()
            response_length = len(response)-1
            print('Extracting Data...')
            for idx, row in enumerate(response):
                print(idx,'/',response_length)

                parentid = row['parentid']
                childid = row['childid']

                all_mappings.append([childid, parentid])

            df = pd.DataFrame(all_mappings, columns=columns)
            df.to_csv(f'{sysvars.gard_files_path}gard_to_gard_mappings.csv', index=False)

        # Invalid Mode
        else:
            raise Exception
        
        self.copy_to_dev_neo4j(mode=self.mode)

    def copy_to_dev_neo4j(self, mode=None):
        """
        Copy the generated mapping file to the development Neo4j server. This is because the mapping file needs to be on the same server to be able to load the CSV file into Neo4j

        :param mode: The database mode. Defaults to the class's mode attribute.
        :raises Exception: If the mode is invalid.
        """
        mode = self.mode
        if not mode: raise Exception ('Note a valid mode')

        if sysvars.db_abbrevs2[mode] == 'ct':
            p = Popen(['scp', f'{sysvars.ct_files_path}trial_to_gard_mappings.csv', f'{sysvars.current_user}@{sysvars.rdas_urls["neo4j-dev"]}:{sysvars.ct_files_path}trial_to_gard_mappings.csv'], encoding='utf8')
            p.wait()

        if sysvars.db_abbrevs2[mode] == 'pm':
            p = Popen(['scp', f'{sysvars.pm_files_path}article_to_gard_mappings.csv', f'{sysvars.current_user}@{sysvars.rdas_urls["neo4j-dev"]}:{sysvars.pm_files_path}article_to_gard_mappings.csv'], encoding='utf8')
            p.wait()

        if sysvars.db_abbrevs2[mode] == 'gnt':
            p = Popen(['scp', f'{sysvars.gnt_files_path}project_to_gard_mappings.csv', f'{sysvars.current_user}@{sysvars.rdas_urls["neo4j-dev"]}:{sysvars.gnt_files_path}project_to_gard_mappings.csv'], encoding='utf8')
            p.wait()

        if sysvars.db_abbrevs2[mode] == 'gard':
            p = Popen(['scp', f'{sysvars.gard_files_path}gard_to_gard_mappings.csv', f'{sysvars.current_user}@{sysvars.rdas_urls["neo4j-dev"]}:{sysvars.gard_files_path}gard_to_gard_mappings.csv'], encoding='utf8')
            p.wait()