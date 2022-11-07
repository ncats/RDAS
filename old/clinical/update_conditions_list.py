import requests
import html

# add names of all rare diseases on clinical trials site to file
x = requests.get('https://clinicaltrials.gov/ct2/search/browse?brwse=ord_alpha_all')
file = open('conditions_list.txt', 'w')
for line in x.text.splitlines():
    if '\"Search for' in line: 
        condition_name = line.split('\"')[2].replace('Search for ','')[:-1]
        condition_name = html.unescape(condition_name)
        file.write(condition_name + '\n')

# close file
print('done updating conditions list!')
file.close()