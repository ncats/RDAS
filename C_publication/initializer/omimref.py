import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
import json
import hashlib
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string, _make_hash_key
 

#This is typically used to quickly generate column lists for INSERT or SELECT statements, especially useful when you want to copy the exact column structure without typing them manually.
#For example: 'id','omim_id','entry_json','processed','created'
f"""    
    SELECT CONCAT('''',  GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ''','''), '''') 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'rdas_db' 
    AND TABLE_NAME = 'publication_omim';
    """

#This SQL script retrieves all column names from a specific table and concatenates them into a single comma-separated string.
f"""
    SELECT CONCAT( GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ',')) 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'rdas_db' 
    AND TABLE_NAME = 'publication_omim';
    """

# Delete duplicate rows
f'''
    use rdas_db;
    DELETE t1 FROM publication_omim AS t1
    WHERE EXISTS (
        SELECT 1
        FROM publication_omim AS t2
        WHERE t2.omim_id = t1.omim_id
        AND t2.id < t1.id
    );
    '''


""" 
    0. Do this first:  initializer/omim_article.py before doing the OMIMRefInitializer
""" 
# 1. Create OMIMRef nodes
class OMIMRefInitializer(InitBase):


    def __init__(self): 

        super().__init__('publication_omim', 'OMIMRef')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # Create index on omimId & omimSections
        self.create_indexes('OMIMRef', ['omimId', 'omimSections', '_composite_key']) 


    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 100):   

        # Why use indexes on both omimId & omimSections?
        # One omimId may be referenced by multiple pubmedId with different omimSections. 
        # The there are may be exist many OMIMRef nodes which have same omimId and omimName but different omimSections
        ''' Example ---
        [
            {
                "pubmedId": 10071185,
                "target_refs": [
                    {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSections": [
                        "Description",
                        "Biochemical Features",
                        "Molecular Genetics"
                    ]
                    }
                ],
                "formatted_today": "2025-12-18"
            },
            {
                "pubmedId": 19296131,
                "target_refs": [
                    {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSections": [
                        "Molecular Genetics"
                    ]
                    }
                ],
                "formatted_today": "2025-12-18"
            }
        ]
        '''

        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (a: Article {pubmedId: chunk.pubmedId})
            UNWIND chunk.target_refs AS ref
            MERGE (o:OMIMRef {_composite_key: ref._composite_key})
            ON CREATE SET 
                o.omimId = ref.omimId,
                o.omimName = ref.omimName,
                o.omimSections = ref.omimSections,
                o.dateCreatedByRDAS = chunk.formatted_today,
                o.lastUpdatedByRDAS = chunk.formatted_today
            MERGE (a)-[:has_omim_ref]->(o)
        '''
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  id, omim_id, entry_json
                FROM  {self.table_name}
                WHERE (id BETWEEN {start_id} AND {end_id}) 
                AND (processed IS NULL or processed != \'{self.processed_flag}\') 
            '''
           
            try: 
                self.dict_cursor.execute(query)
                rows = self.dict_cursor.fetchall()

                chunks = []
                for row in rows: 

                    id = row['id']
                    omim_id = row['omim_id']
                    entry_json = row['entry_json']
                    print('. ', end='', flush=True)                    

                    result_obj = self.parse_entry_json(id, omim_id, entry_json)

                    pubmed_omimrefs_mapping_list = self.get_pubmed_id_and_omimrefs_mapping_list(result_obj)

                    if pubmed_omimrefs_mapping_list:
                        chunks.extend(pubmed_omimrefs_mapping_list)
                
                if chunks:
                    _count += len(chunks)                     
                     
                    try:
                        self.memgraph.execute(batch_create, {"chunks": chunks}) 
                    except Exception as e:
                        self.appender.append_and_print(f'Exception while insert: {e}')
                        raise 

                    self.appender.log_stdout(f'\n{_curr_timestamp()} [total: {_count}], [flag: {self.processed_flag}, Id range: [{start_id} - {end_id}], #OMIMRefs chunks = {len(chunks)}')  

                self.update_processed_flag(start_id, end_id, self.processed_flag)

            except Exception as e:
                self.appender.append_and_print(f'Error: {e}')
        
        self.close_mysql_conn() 
        
        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close()  
   

    def get_omimrefs_by_ref_num(self, omimref_obj_list, ref_num):

        target_refs = []
        for obj in omimref_obj_list:
            if str(ref_num) in obj['ref_nums']:
                target_refs.append(obj)

        return target_refs

 
    def get_pubmed_id_and_omimrefs_mapping_list(self, result_obj):
    
        pubmed_omimrefs_mapping_list = []
        txt_section_obj_list = result_obj.get('textSectionList', [])

        omimref_obj_list = []

        for obj in txt_section_obj_list:

            omimref_obj_list.append({
                "omimId": result_obj.get('omim_id', ''),
                "omimName": result_obj.get('title', ''),
                "omimSection": obj.get('title', ''),
                "ref_nums": obj.get('ref_nums', [])
            }) 
    
        reference_obj_list = result_obj.get('referenceList', [])

        for obj in reference_obj_list:
            
            ref_num = obj.get('referenceNumber')
            if ref_num is None:
                continue
                
            target_refs = self.get_omimrefs_by_ref_num(omimref_obj_list, ref_num)

            if obj.get('pubmedID'):

                pubmed_id = obj['pubmedID']
                '''
                    1. If  pubmed_id = obj['pubmedID'] is valid, and already in publication_article table, do nothing.
                '''

                ''' 
                    2. If the pubmed_id = obj['pubmedID'] is valid, and is not in publication_article table
                    See: init_9_publication-add-omim-article.py

                    {
                        1. If pubmed_is is not in the publication_article table
                        2. Get article by pubmed_id
                            # https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:5770408&resultType=core&format=json&pageSize=1000
                            # https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:8314082&resultType=core&format=json&pageSize=1000

                        3. Save the article into publication_article table
                    }               
                '''         

                '''
                    3. Add the article to the Publication in Memgraph:
                        See 3_publication/init_19_publication-graph-Articles.py :: step_1_create_pubmed_article()
                '''       

                '''
                    4. We don't need add the article to the Publication in Memgraph here.
                    5. We just need add refs here
                '''
    
                if target_refs:

                    transformed_target_refs = self.transform_target_refs(target_refs) 
                    
                    pubmed_omimrefs_mapping_list.append({
                        "pubmedId": pubmed_id,
                        "target_refs": transformed_target_refs,
                        "formatted_today": self.formatted_today
                    })
                    
        return pubmed_omimrefs_mapping_list
    


    def transform_target_refs(self, target_refs):
        
        # The input target_refs
        '''
        {
            "pubmedId": 10071185,
            "target_refs": [
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSection": "Description",
                    "ref_nums": [ "18", "7" ]
                },
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSection": "Biochemical Features",
                    "ref_nums": [ "2", "20", "18", "4" ]
                },
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSection": "Molecular Genetics",
                    "ref_nums": [ "20", "17", "16", "10", "18" ]
                }
            ],
            "formatted_today": "2025-12-18"
            }
        '''
        # The output target_refs
        '''
        {
            "pubmedId": 10071185,
            "target_refs": [
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSections": [
                        "Description",
                        "Biochemical Features",
                        "Molecular Genetics"
                    ]
                }
            ],
            "formatted_today": "2025-12-18"
            }
        '''
        
        omim_dict = {}
        
        for ref in target_refs:
            omim_id = ref.get('omimId')
            omim_name = ref.get('omimName', '')
            omim_section = ref.get('omimSection', '')
            
            if omim_id:
                if omim_id not in omim_dict:
                    omim_dict[omim_id] = {
                        'omimId': omim_id,
                        'omimName': omim_name,
                        'omimSections': []
                    }
                
                # Add section if it's not empty and not already in the list
                if omim_section and omim_section not in omim_dict[omim_id]['omimSections']:
                    omim_dict[omim_id]['omimSections'].append(omim_section)
        
        ''' Sort the omimSections, the omimSections will be used as one of indexes'''
        # Sort omimSections for each omimId
        for omim_id in omim_dict:
            omim_dict[omim_id]['omimSections'].sort()

            # Create composite key from omimId and omimSections
            composite_key_str = f"{omim_id}{'-'.join(omim_dict[omim_id]['omimSections'])}"
            composite_key_str = "".join(composite_key_str.lower().split())  # Replace whitespaces
            
            # Hash the composite key
            _composite_key = _make_hash_key(composite_key_str)

            omim_dict[omim_id]['_composite_key'] = _composite_key

        # Convert dictionary values to list
        consolidated_refs = list(omim_dict.values())
        
        return consolidated_refs


    def parse_entry_json(self, id, omim_id, entry_json):
        '''
        {
            "omim": {
                "version": "1.0",
                "entryList": [
                {
                    "entry": {
                    "prefix": "#",
                    "mimNumber": 200110,
                    "status": "live",
                    "titles": {
                        "preferredTitle": "ABLEPHARON-MACROSTOMIA SYNDROME; AMS"
                    },
                    "textSectionList": [
                        {
                        "textSection": {
                            "textSectionName": "text",
                            "textSectionTitle": "Text",
                            "textSectionContent": "A number sign (#) is used with this entry because of evidence that ablepharon-macrostomia syndrome (AMS) is caused by heterozygous mutation in the TWIST2 gene ({607556}) on chromosome 2q37."
                        }
                        },
                        {
                        "textSection": {
                            "textSectionName": "description",
                            "textSectionTitle": "Description",
                            "textSectionContent": "Ablepharon-macrostomia syndrome (AMS) is a congenital ectodermal dysplasia characterized by absent eyelids, macrostomia, microtia, redundant skin, sparse hair, dysmorphic nose and ears, variable abnormalities of the nipples, genitalia, fingers, and hands, largely normal intellectual and motor development, and poor growth (summary by {7:Marchegiani et al., 2015})."
                        }
                        },
                        {
                        "textSection": {
                            "textSectionName": "inheritance",
                            "textSectionTitle": "Inheritance",
                            "textSectionContent": "{13:Rohena et al. (2011)} reported AMS in a newborn female and her 22-year-old father and suggested autosomal dominant inheritance of the disorder. {7:Marchegiani et al. (2015)} confirmed autosomal dominant inheritance of the AMS in this family.\n\nPossible autosomal recessive inheritance was proposed because of a postulated relationship to the disorder in monozygotic twins from a consanguineous marriage: one twin had bilateral cryptophthalmos and the other had cryptophthalmos on the left and ablepharon on the right ({1:Azevedo et al., 1973})."
                        }
                        },
                        {
                        "textSection": {
                            "textSectionName": "cytogenetics",
                            "textSectionTitle": "Cytogenetics",
                            "textSectionContent": "{11:Pellegrino et al. (1996)} described a male infant with ablepharon-macrostomia syndrome and a complex rearrangement and partial deletion of chromosome 18; the final karyotype, based on molecular cytogenetic analysis, was 46,XY,-18,+[del(18)(q21.3q23),inv(18)(q12.3q21.2)]. The authors stated that this was the first AMS patient to be reported with an abnormal karyotype, and that he lacked the typical features of the 18q deletion syndrome ({601808}). {11:Pellegrino et al. (1996)} suggested that the gene(s) for ablepharon-macrostomia syndrome might lie on chromosome 18 in the region of this patient's deletion or inversion breakpoints."
                        }
                        }
                    ],
                    "referenceList": [
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 1,
                            "authors": "Azevedo, E. S., Biondi, J., Ramalho, L. M.",
                            "title": "Cryptophthalmos in two families from Bahia, Brazil.",
                            "source": "J. Med. Genet. 10: 389-392, 1973.",
                            "pubmedID": 4774831,
                            "doi": "10.1136/jmg.10.4.389"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 2,
                            "authors": "Brancati, F., Mingarelli, R., Sarkozy, A., Dallapiccola, B.",
                            "title": "Ablepharon-macrostomia syndrome in a 46-year-old woman.",
                            "source": "Am. J. Med. Genet. 127A: 96-98, 2004.",
                            "pubmedID": 15103726,
                            "doi": "10.1002/ajmg.a.20658"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 5,
                            "authors": "Hornblass, A., Reifler, D. M.",
                            "title": "Ablepharon macrostomia syndrome.",
                            "source": "Am. J. Ophthal. 99: 552-556, 1985.",
                            "pubmedID": 4003491,
                            "doi": "10.1016/s0002-9394(14)77956-5"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 10,
                            "authors": "McCarthy, G. T., West, C. M.",
                            "title": "Ablepheron (sic) macrostomia syndrome.",
                            "source": "Dev. Med. Child Neurol. 19: 659-672, 1977.",
                            "pubmedID": 913905,
                            "doi": "10.1111/j.1469-8749.1977.tb07999.x"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 11,
                            "authors": "Pellegrino, J. E., Schnur, R. E., Boghosian-Sell, L., Strathdee, G., Overhauser, J., Spinner, N. B., Stump, T., Grace, K., Zackai, E. H.",
                            "title": "Ablepharon macrostomia syndrome with associated cutis laxa: possible localization to 18q.",
                            "source": "Hum. Genet. 97: 532-536, 1996.",
                            "pubmedID": 8834257,
                            "doi": "10.1007/BF02267081"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 14,
                            "authors": "Stevens, C. A., Sargent, L. A.",
                            "title": "Ablepharon-macrostomia syndrome.",
                            "source": "Am. J. Med. Genet. 107: 30-37, 2002.",
                            "pubmedID": 11807864,
                            "doi": "10.1002/ajmg.10123"
                        }
                        }
                    ]
                    }
                }
                ]
            }
        }
        '''
        # Regular expression pattern to match just the number between { and :
        pattern = r'{(\d+):'

        omimObj = json.loads(entry_json)  

        result_obj = {"id": id, "omim_id": omim_id, "title": "", "textSectionList": [], "referenceList": [] }
        
        if omimObj and omimObj.get('omim', {}).get('entryList'):
            entry_list = omimObj['omim']['entryList']

            if len(entry_list) > 0:
                entry = entry_list[0]['entry']

                # Extract title
                if entry.get('titles', {}).get('preferredTitle'):
                    result_obj['title'] = entry['titles']['preferredTitle']
                
                # Extract text sections
                if entry.get('textSectionList'):

                    txt_section_obj_list = []
                    for item in entry['textSectionList']:

                        txt_section = item['textSection']
                        txt_section_content = txt_section.get('textSectionContent', '')
                        ref_nums = re.findall(pattern, txt_section_content)
                        
                        txt_section_obj_list.append({
                            "title": txt_section.get('textSectionTitle', ''),
                            "ref_nums": list(set(ref_nums))
                        })

                    result_obj['textSectionList'] = txt_section_obj_list
                
                # Extract references
                if entry.get('referenceList'):

                    reference_obj_list = []
                    for item in entry['referenceList']:
                        reference = item['reference']
                        reference_obj_list.append(reference)
                        
                    result_obj['referenceList'] = reference_obj_list

        return result_obj

 