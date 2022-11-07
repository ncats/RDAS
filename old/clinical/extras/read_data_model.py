'''
code that generates code
read data_model.csv and define each node type as a list in python
then, print
'''

# data model
data_model = dict()

# read data model file
read_file = open('data_model.csv', 'r')#, encoding="utf-8")
lines = read_file.readlines()
read_file.close()

# fill data model
for line in lines[1:]:
    line = line.split(',')
    node_type = line[0].replace(' ', '')
    field_name = line[2]
    if len(node_type) > 0:
        if node_type not in data_model:
            data_model[node_type] = list()
        data_model[node_type].append(field_name)
            
# print data model:
max_char = 120  # max number of characters per line
max_char -= 1   # adjust
tab_size = 4

# print class names
start = 'class_names = ['
print(start, end='')
line_char_count = len(start)
class_names = [node_type for node_type in data_model]
for class_name in class_names[:-1]:
    
    if line_char_count + len(class_name) + 3 < max_char:
        print('\'' + class_name + '\',', end='')
        line_char_count += len(class_name) + 3
    else:
        print()
        print(' ' * tab_size + '\'' + class_name + '\',', end='')
        line_char_count = tab_size + 3 + len(class_name)
if line_char_count + len(class_names[-1]) + 3 >= max_char:
    print('\n' + ' ' * tab_size, end='')
print('\'' + class_names[-1] + '\']\n\n', end='')

# print fields lists
for node_type in data_model:
    fields = data_model[node_type]
    line_char_count = len(node_type) + 4
    print(node_type, '= [', end='')
    for field in fields[:-1]:
        if line_char_count + len(field) + 3 < max_char:
            print('\'' + field + '\',', end='')
            line_char_count += len(field) + 3
        else:
            print()
            print(' ' * tab_size + '\'' + field + '\',', end='')
            line_char_count = tab_size + 3 + len(field)
    if line_char_count + len(fields[-1]) + 3 >= max_char:
        print('\n' + ' ' * tab_size, end='')
    print('\'' + fields[-1] + '\']\n\n', end='')
    
# print class types
start = 'class_types = ['
print(start, end='')
line_char_count = len(start)
class_names = [node_type for node_type in data_model]
for class_name in class_names[:-1]:
    
    if line_char_count + len(class_name) + 1 < max_char:
        print(class_name + ', ', end='')
        line_char_count += len(class_name) + 2
    else:
        print()
        print(' ' * tab_size + class_name + ', ', end='')
        line_char_count = tab_size + 2 + len(class_name)
if line_char_count + len(class_names[-1]) + 3 >= max_char:
    print('\n' + ' ' * tab_size, end='')
print(class_names[-1] + ']\n\n', end='')
