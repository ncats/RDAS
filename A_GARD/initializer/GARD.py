# Add the project root to the Python path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.init_base import InitBase
from utils.file_appender import FileAppender
from utils.tools import _date_string, _set_value_for_none

'''
    See the previous version: 1_GARD/init_2_GARD_step_2.py
'''

class GARDInitializer(InitBase):


    def __init__(self): 

        super().__init__('gard', 'GARD nodes')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/1-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    def init_nodes(self):

        cypher = """
            UNWIND $chunks AS chunk
            MERGE (d:GARD {gardId: chunk.gardId})
            ON CREATE SET
                d.gardName = chunk.gardName,
                d.classificationLevel = chunk.classificationLevel,
                d.disorderType = chunk.disorderType,
                d.synonyms = chunk.synonyms
            ON MATCH SET
                d.gardName = chunk.gardName,
                d.classificationLevel = chunk.classificationLevel,
                d.disorderType = chunk.disorderType,
                d.synonyms = chunk.synonyms
                // , d.xrefs = chunk.xrefs   // uncomment if you want to store xrefs too
        """

        batch_size=100
 
        query = f'''
            SELECT 
                GardID, MONDO_ID, 
                GROUP_CONCAT(distinct ORPHA_Code) as orphaCode, 
                GROUP_CONCAT(distinct Classification_Level) as classificationLevel, 
                GROUP_CONCAT(distinct Disorder_Type) as disorderType,
                MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END) AS `name`,
                GROUP_CONCAT(CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END SEPARATOR '$$$') AS `synonyms`,
                GROUP_CONCAT(Label_Xref SEPARATOR ',') AS `xrefs`,
                Label_Source
            FROM  {self.table_name}
            WHERE 
                Label_Predicate_Mapping != 'DEPRECATED' 
                AND LENGTH(Label) > 3
            GROUP BY 
                GardID, MONDO_ID, Label_Source
        '''

        cursor = self.mysql.cursor(dictionary=True, buffered=True)
        cursor.execute(query)

        count = 0
        # 0. 
        while True:

            try:            
                # Fetch rows by the fetch batch_size
                rows = cursor.fetchmany(batch_size)

                if not rows:
                    self.appender.log_stdout(f'\n--- All finished, no more data ---')
                    break
              
                # Prepare parameter rows
                chunks = []
                for row in rows:

                    row = _set_value_for_none(row)

                    chunks.append({
                        "gardId": row["GardID"],
                        "gardName": row["name"],
                        #"dataSource": row["Label_Source"],
                        #"dataSourceId": row["MONDO_ID"],
                        "classificationLevel": (row["classificationLevel"].split(",") if row["classificationLevel"] else []),
                        "disorderType": (row["disorderType"].split(",") if row["disorderType"] else []),
                        "synonyms": (row["synonyms"].split("$$$") if row["synonyms"] else []),
                        # keep xrefs handy if you want to store later
                        "xrefs": (row["xrefs"].split(",") if row["xrefs"] else [])
                    })

                try:
                    # If your client supports params: memgraph.execute(cypher, {"chunks": chunks})
                    # If not, fall back to embedding JSON 
                    self.memgraph.execute(cypher, {"chunks": chunks})

                    count += len(chunks)
                    self.appender.log_stdout(f"✅ Successfully created {count} GARD nodes.")
        
                except Exception as e:
                    self.appender.log_stdout(f"Batch insert into Memgraph failed: {e}") 
            
            except Exception as error:
                self.appender.log_stdout(f"❌ Failed to read data from MySQL table: {error}")


        # 1. Set extra Disease lable
        try: 
            self.memgraph.execute('MATCH (n:GARD)  SET n:Disease') 
            self.appender.log_stdout(f"✅ Successfully added Disease label to GARD nodes.")

        except Exception as e:
            self.appender.log_stdout(f"Add Disease label to GARD nodes failed: {e}") 

        # 2. Add statistics properties
        try: 
            stat_cypher = """
                MATCH (n:GARD) 
                SET n += {countArticles: 0, countProjects: 0, countTrials: 0, countGenes: 0, countPhenotypes: 0}
            """
            self.memgraph.execute(stat_cypher) 

            self.appender.log_stdout(f"✅ Successfully added statistics properties to GARD nodes.")

        except Exception as e:
            self.appender.log_stdout(f"Add statistics properties to GARD nodes failed: {e}") 


        #Close the MySQL connection
        self.close_mysql_conn() 
        self.appender.log_stdout(f"\n--- MySQL connection is closed ---")
        self.appender.close()