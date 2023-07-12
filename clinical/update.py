from AlertCypher import AlertCypher
from src import data_model as dm 
import methods as rdas
from datetime import date
import init

def main():
    today = date.today().strftime('%m/%d/%y')
    db = AlertCypher('clinicaltest')
    refreshed_ctgov_trials = list()

    ctgov_diseases,listed_trials = rdas.webscrape_ctgov_diseases()    
    for idx,ct_disease in enumerate(ctgov_diseases):
        ctgov_trials = rdas.get_nctids([ct_disease])
        refreshed_ctgov_trials.extend(ctgov_trials)
        print(idx)

    response = db.run('MATCH (x:ClinicalTrial) RETURN x.NCTId,x.LastUpdatePostDate').data()
    current_nctids = {i['x.NCTId']:i['x.LastUpdatePostDate'] for i in response}

    ids_to_update = list()
    length = len(current_nctids)
    ids_to_add = list()

    for idx,(k,v) in enumerate(current_nctids.items()):
        if not k in refreshed_ctgov_trials:
            ids_to_add.append(k)
        else:
            ids_to_update.append(k)
        print(str(idx) + '/' + str(length))

    print('Checking ' + str(len(ids_to_update)) + ' Trials for Updates')
    print('Adding ' + str(len(ids_to_add)) + ' Brand New Trials')

    for idx,ID in enumerate(ids_to_update):
        print(idx)
        trial_info = rdas.extract_fields(ID)
        if trial_info:
            #print(trial_info)
            if not trial_info['LastUpdatePostDate'] == current_nctids[ID]:
                print(trial_info['LastUpdatePostDate'])
                print(current_nctids[ID])

                data_string = rdas.format_node_data(db,today,trial_info,'ClinicalTrial',return_single=True)

                print(ID)
                print(data_string)
                print(today)

                create_history_query = 'MATCH (x:ClinicalTrial {{NCTId:\"{ID}\"}}) CREATE (y:History) SET y=properties(x) CREATE (z:ClinicalTrial {data_string}) MERGE (y)<-[:updated_from]-(x) SET x=properties(z) SET x.DateCreatedRDAS=\"{today}\" SET x.LastUpdatedRDAS=\"{today}\" DELETE z return y'.format(ID=ID,data_string=data_string,today=today)
                #.format(ID=ID,data_string=data_string,today=today)

                print(create_history_query)
                db.run(create_history_query)
                
        else:
            print('Error in update for finding full trial data for ' + ID)

    for ID in ids_to_add:
        trial_info = rdas.extract_fields(ID)
        if trial_info:
            for node_type in dm.node_names:
                data_string = rdas.format_node_data(db,today,trial_info,node_type)

        else:
            print('Error in add for finding full trial data for ' + ID)

if __name__ == "__main__":
    main()
