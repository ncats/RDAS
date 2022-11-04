from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import numpy as np

# connect to neo4j database
print('start')
connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'tgcbf'))
session = connection.session()

# query def
query = 'Match (n:ClinicalTrial) return n.NCTId,n.StartDate,n.Phase'
response = session.run(query)

# months
months = dict()
months['december'], months['january'], months['february']   = 12, 1, 2
months['march'], months['april'], months['may']             = 3, 4, 5
months['june'], months['july'], months['august']            = 6, 7, 8
months['september'], months['october'], months['november']  = 9, 10 , 11

# create list of dates
phase_1_start_dates = list()
phase_2_start_dates = list()
phase_3_start_dates = list()
phase_4_start_dates = list()
unavailable_phase = list()

num_null = 0
for elm in response:
    if elm == None or elm[1] == None:
        num_null += 1
    else:
        month = elm[1].split(' ')[0].lower()
        year = int(elm[1][-4:])
        if month in months:
            if elm[2] == None:
                unavailable_phase.append(year)
            else:
                if str(1) in elm[2][0]: phase_1_start_dates.append(year)
                elif str(2) in elm[2][0]: phase_2_start_dates.append(year)
                elif str(3) in elm[2][0]: phase_3_start_dates.append(year)
                elif str(4) in elm[2][0]: phase_4_start_dates.append(year)
                else: unavailable_phase.append(year)
        else:
            num_null += 1
print(num_null, 'clinical trials with no data')
print('\t-->null trial percentage', np.round(100 * num_null / 179652, 3))
            
# create histogram
binss = [i for i in range(1980,2030)]
n_1, bins,_ = plt.hist(phase_1_start_dates, bins=binss)
n_2, bins,_ = plt.hist(phase_2_start_dates, bins=bins)
n_3, bins,_ = plt.hist(phase_3_start_dates, bins=bins)
n_4, bins,_ = plt.hist(phase_4_start_dates, bins=bins)
n_0, bins,_ = plt.hist(unavailable_phase, bins=bins)

plt.show()

# create lineplot
order = [n_1,n_2,n_3,n_4,n_0]
sum_prev = np.array([0 for _ in n_1])
for i in range(len(order)):
    
    colorr = '#9E9E9E'
    labell = 'none'
    if i == 0: 
        colorr = '#7E57C2'
        labell = 'Phase 1'
    if i == 1: 
        colorr = '#42A5F5'
        labell = 'Phase 2'
    if i == 2: 
        colorr = '#66BB6A'
        labell = 'Phase 3'
    if i == 3: 
        colorr = '#EF5350'
        labell = 'Phase 4'
    if i == 4: 
        colorr = '#E0E0E0'
        labell = 'Phase N/A'
    
    sum_next = np.add(sum_prev, order[i])
    plt.fill_between(bins[:len(n_1)], sum_prev, sum_next,color=colorr,label=labell)
    sum_prev = sum_next

plt.xlim([1980,2022])
plt.title('Current Phase of Rare Disease Clinical Trial by Start Year')
plt.xlabel('Start Year')
plt.legend(loc = 'upper left')
plt.ylabel('Number of Clinical Trials')
plt.show()
