from AlertCypher import AlertCypher
import clinical.generate, grant.generate, pubmed.generate, gard.generate
import threading
from datetime import datetime, date
from time import sleep

def populate(db):
    threads = list()
    try:
        response = db.run("MATCH (x:GARD) RETURN x LIMIT 1").single()
        if response == None:
            print('GARD Database Empty')
            gard.generate.check(True, db)
    except:
        pass
    try:
        response = db.run("MATCH (x:CT_ClinicalTrial) RETURN x LIMIT 1").single()
        if response == None:
            print('Clinical Trial Database Empty')
            thread = threading.Thread(target=clinical.generate.check, args=(True, db,), daemon=True)
            threads.append(thread)
    except:
        pass
    try:
        response = db.run("MATCH (x:PM_Article) RETURN x LIMIT 1").single()
        if response == None:
            print('PubMed Database Empty')
            thread = threading.Thread(target=pubmed.generate.check, args=(True, db,), daemon=True)
            threads.append(thread)
    except:
        pass
    try:
        response = db.run("MATCH (x:GNT_Project) RETURN x LIMIT 1").single()
        if response == None:
            print('Grant Database Empty')
            thread = threading.Thread(target=grant.generate.check, args=(True, db,), daemon=True)
            threads.append(thread)
    except:
        pass

    for thread in threads:
        if thread:
            thread.start()

    for thread in threads:
        if thread:
            thread.join()

def updateCT(db):
    update = db.getConf('DATABASE', 'clinical_update')
    update = datetime.strptime(update,"%m/%d/%y")

    current_time = datetime.now()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.strptime(current_time,"%m/%d/%y")

    delta = current_time - update

    interval = int(db.getConf("DATABASE", "clinical_interval"))
    if delta.days > interval:
        return True
    else:
        return False

def updatePM(db):
    update = db.getConf('DATABASE', 'pubmed_update')
    update = datetime.strptime(update,"%m/%d/%y")

    current_time = datetime.now()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.strptime(current_time,"%m/%d/%y")

    delta = current_time - update

    interval = int(db.getConf("DATABASE", "pubmed_interval"))
    if delta.days > interval:
        return True
    else:
        return False

def updateGNT(db):
    update = db.getConf('DATABASE', 'grant_update')
    update = datetime.strptime(update,"%m/%d/%y")

    current_time = datetime.now()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.strptime(current_time,"%m/%d/%y")

    delta = current_time - update

    interval = int(db.getConf("DATABASE", "grant_interval"))
    if delta.days > interval:
        return True
    else:
        return False
        
def updateCheck(db):
    update_list = {'clinical':False, 'pubmed':False, 'grant':False,}

    update_list['clinical'] = updateCT(db)
    update_list['pubmed'] = updatePM(db)
    update_list['grant'] = updateGNT(db)

    threads = list()
    for k,v in update_list.items():
        if v == True:
            if k == 'clinical':
                thread = threading.Thread(target=clinical.generate.check, args=(False, db,), daemon=True)
            elif k == 'pubmed':
                thread = threading.Thread(target=pubmed.generate.check, args=(False, db,), daemon=True)
            elif k == 'grant':
                thread = threading.Thread(target=grant.generate.check, args=(False, db,), daemon=True)
    
    for thread in threads:
        if thread:
            thread.start()

    for thread in threads:
        if thread:
            thread.join()
    
    return update_list

def updateDate(db, info):
    date = datetime.now().strftime("%m/%d/%y")
    for k,v in info.items():
        if v == True:
            if k == 'clinical':
                db.setConf("DATABASE", "clinical_update", date)
            elif k == 'pubmed':
                db.setConf("DATABASE", "pubmed_update", date)
            elif k == 'grant':
                db.setConf("DATABASE", "grant_update", date)
        
def updateGard(db):
    update = db.getConf('DATABASE', 'gard_update')
    update = datetime.strptime(update,"%m/%d/%y")

    current_time = datetime.now()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.strptime(current_time,"%m/%d/%y")

    delta = current_time - update

    interval = int(db.getConf("DATABASE", "gard_interval"))
    if delta.days > interval:
        gard.generate.check(False, db)
        db.setConf("DATABASE", "gard_update", current_time)

rdas = AlertCypher("rdas")
populate(rdas)

while True:
    updateGard(rdas)
    updateDate(rdas, updateCheck(rdas))
    sleep(5)

