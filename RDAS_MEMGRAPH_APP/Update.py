import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
import sysvars
from datetime import datetime
from AlertCypher import AlertCypher
from subprocess import *
from time import sleep
import argparse

class Update:
    def __init__ (self, mode=None):
        """
        Initialize the Update class with the specified mode.

        :param mode: The database mode (e.g., 'ct', 'pm', 'gnt', 'gard').
        :raises Exception: If the mode is not in sysvars.dump_dirs.
        """
        if mode in sysvars.dump_dirs:
            self.mode = mode
            self.db = AlertCypher(mode)
        else:
            raise Exception('Not in sysvars')
        
    def refresh_node_counts(self):
        """
        Refresh node counts for various relationships (e.g., trials, articles, projects) in the GARD database.

        Uses:
        - GARD database to set initial counts to 0.
        - Other related databases to compute counts for specific relationships.
        """
        def populate_node_counts(db,data,prop_name):
            """
            Helper function to populate node counts in the GARD database.

            :param db: Database connection to update.
            :param data: List of count data to update nodes with.
            :param prop_name: Property name to set the count (e.g., 'COUNT_TRIALS').
            """
            for row in data:
                gard_id = row['gard_id']
                cnt = row['cnt']
                query = 'MATCH (x:GARD) WHERE x.GardId = \"{gard_id}\" SET x.{prop_name} = {cnt}'.format(gard_id=gard_id,cnt=cnt,prop_name=prop_name)
                db.run(query)

        db = AlertCypher(sysvars.gard_db)
        ct_db = AlertCypher(sysvars.ct_db)
        pm_db = AlertCypher(sysvars.pm_db)
        gnt_db = AlertCypher(sysvars.gnt_db)

        # Reset all counts to 0 in the GARD database
        db.run('MATCH (x:GARD) SET x.COUNT_GENES = 0 SET x.COUNT_PHENOTYPES = 0 SET x.COUNT_TRIALS = 0 SET x.COUNT_ARTICLES = 0 SET x.COUNT_PROJECTS = 0')

        # Update counts for each relationship type
        db.run('MATCH (x:GARD)--(y:Phenotype) WITH COUNT(DISTINCT y) AS cnt,x SET x.COUNT_PHENOTYPES = cnt').data()
        db.run('MATCH (x:GARD)--(y:Gene) WITH COUNT(DISTINCT y) AS cnt,x SET x.COUNT_GENES = cnt').data()
        res3 = ct_db.run('MATCH (x:GARD)--(ct:ClinicalTrial) WITH COUNT(DISTINCT ct) AS cnt,x RETURN cnt AS cnt,x.GardId AS gard_id').data()
        res4 = pm_db.run('MATCH (x:GARD)--(y:Article) WITH COUNT(DISTINCT y) AS cnt,x RETURN cnt AS cnt, x.GardId AS gard_id').data()
        res5 = gnt_db.run('MATCH (x:GARD)--(y:Project)--(z:CoreProject) WITH COUNT(DISTINCT z) AS cnt,x RETURN cnt AS cnt, x.GardId as gard_id').data()

        # Populate the calculated counts into the GARD database
        populate_node_counts(db,res3,'COUNT_TRIALS')
        populate_node_counts(db,res4,'COUNT_ARTICLES')
        populate_node_counts(db,res5,'COUNT_PROJECTS')
        
    def check_update(self):
        """
        Checks if an update is needed for the current database mode based on update intervals.

        :return: A list containing:
                 - A boolean indicating if an update is needed.
                 - The last update date as a string.

        """
        # Get the current date and time
        today = datetime.now()

        selection = sysvars.update_check_fields[self.mode]
        print("selection::",selection)

        # Get the last update date from the configuration
        last_update = self.db.getConf('UPDATE_PROGRESS',selection[0])
        last_update = datetime.strptime(last_update,"%m/%d/%y")
        print("last_update::",last_update)

        # Calculate the time difference between today and the last update
        delta = today - last_update

        interval = self.db.getConf('DATABASE',selection[1])
        interval = int(interval)
        print("interval::",interval)

        # Get the update interval from the configuration
        last_update = datetime.strftime(last_update,"%m/%d/%y")

        # Check if an update is needed based on the interval
        if delta.days > interval:
            return [True,last_update]
        else:
            return [False,last_update]