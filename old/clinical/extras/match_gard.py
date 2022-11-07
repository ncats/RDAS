# put rare disease names (from clinical trials website) into set
import csv
diseases = set()
file = open('conditions_list.txt', 'r', encoding="utf-8")
diseases_list = file.readlines()
for disease in diseases_list:
    diseases.add(disease[:-1].lower())
file.close()

# count
num_exact = 0
num_synonym = 0

write_file = open('conditions_matched_2.csv', 'w', encoding="utf-8")
write_file.write('gard_id,gard_name,disease_name,found_by,all_synonyms\n')

# search through ID list, add to dictionary
mapping = dict()
file = open('GARDId.csv', 'r', encoding="utf-8")
ID_list = file.readlines()
for line in csv.reader(ID_list):
    
    if line[1].lower() in diseases:
        num_exact += 1
        diseases.remove(line[1].lower())
        mapping[line[1].lower()] = line[0]
        write_file.write(line[0] + ',\"' + line[1] + '\",\"' + line[1].lower() + '\",' + 'exact_match,\"' + line[2] + '\"\n')
        
    else:
        
        # debug
        print('couldnt find:', line[1], '--> search synonyms')
        print(line[2][1:-1])
        
        # form synonyms list
        split_list = [elm.strip() for elm in line[2][1:-1].split(',')]
        synonyms = list()
        for i in range(len(split_list)):
            if len(split_list) != 0 and len(split_list[i]) != 0 and split_list[i][0].islower() and len(synonyms) != 0:
                synonyms[-1] += ', ' + split_list[i]
            else:
                synonyms.append(split_list[i])

        # search for synonyms
        for synonym in synonyms:
            print('\tsynonym:', synonym.strip())
            if synonym.strip() in diseases:
                print('\t\t--> match!')
                num_synonym += 1
                diseases.remove(synonym.strip())
                mapping[synonym] = line[0]
                write_file.write(line[0] + ',\"' + line[1] + '\",\"' + synonym.strip() + '\",' + 'synonym,\"' +  line[2] + '\"\n')
        print()
        
file.close()

print()
print('num_exact', num_exact)
print('num_synonym', num_synonym)

print(len(diseases), 'diseases were not found. Listed below:')
for disease in diseases: 
    print('\t', disease)
    write_file.write('-,-,\"' + disease + '\",None,not_found,-\n')
            
    
