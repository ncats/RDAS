import os

current_version = 2.6

# Basic user information
current_user = 'leadmandj'
base_directory_name = 'RDAS'
# base_path = '/home/{current_user}/RDAS_master/{base_directory_name}/'.format(current_user=current_user, base_directory_name=base_directory_name)

# RDAS Team contacts for emails
contacts = [
    "leadmandj@nih.gov"
]

base_path = '/home/{current_user}/{base_directory_name}/'.format(current_user=current_user, base_directory_name=base_directory_name)

# RDAS Team contacts for emails
contacts = [
    "leadmandj@nih.gov"
]

# Folder paths
backup_path = '{base_path}backup/'.format(base_path=base_path)
transfer_path = '{base_path}transfer/'.format(base_path=base_path)
migrated_path = '{base_path}migrated/'.format(base_path=base_path)
approved_path = '{base_path}approved/'.format(base_path=base_path)
images_path = '{base_path}img/'.format(base_path=base_path)
firebase_key_path = '{base_path}crt/ncats-summer-interns-firebase-adminsdk-9g7zz-a4e783d24c.json'.format(base_path=base_path) # May have to set this in new enviroment


# if you are not using minghui's test dataset, make db_prefix=""; now you only need to change the neo4j database names here:
db_prefix=""
ct_db_name="new.rdas.ctkg" 
gf_db_name='rdas.gfkg'
pa_db_name="rdas.pakg"
gard_db_name='test.rdas.gard'

ct_db = db_prefix+ct_db_name
pm_db = db_prefix+pa_db_name
gnt_db = db_prefix+gf_db_name
gard_db = db_prefix+gard_db_name

# Conversions
dump_dirs = ['RDAS.CTKG','RDAS.PAKG','RDAS.GFKG','RDAS.GARD']
db_abbrevs = {'ct':'RDAS.CTKG', 'pm':'RDAS.PAKG', 'gnt':'RDAS.GFKG'}
db_abbrevs2 = {ct_db:'ct', pm_db:'pm', gnt_db:'gnt'}

# Paths to database creation and update source files
ct_files_path = '{base_path}RDAS_CTKG/src/'.format(base_path=base_path)
pm_files_path = '{base_path}RDAS_PAKG/src/'.format(base_path=base_path)
gnt_files_path = '{base_path}RDAS_GFKG/src/'.format(base_path=base_path)
gard_files_path = '{base_path}RDAS_GARD/src/'.format(base_path=base_path)

# Database names being used on the current server
convert = {ct_db:'trials', pm_db:'articles', gnt_db:'grants'}

# Server URLS and addresses # Original epiapi_url is https://rdas.ncats.nih.gov/api/epi/
epiapi_url = "https://rdas.ncats.nih.gov/api/epi/"
rdas_urls = {'neo4j-dev':"ncats-neo4j-lnx-dev.ncats.nih.gov",'dev':"rdas-dev.ncats.nih.gov",'test':"ncats-neo4j-lnx-test1.ncats.nih.gov",'prod':"ncats-neo4j-lnx-prod1.ncats.nih.gov"}

# GARD exclusion list for when GARD-Project mappings are made in the grant code
gard_preprocessor_exclude = [
'GARD:10311', 'GARD:10984', 'GARD:12351', 'GARD:12352', 'GARD:12638',
'GARD:12915', 'GARD:12976', 'GARD:12977', 'GARD:15010', 'GARD:15042',
'GARD:15066', 'GARD:15076', 'GARD:15080', 'GARD:15092', 'GARD:15112',
'GARD:15119', 'GARD:15191', 'GARD:15192', 'GARD:15211', 'GARD:15300',
'GARD:15315', 'GARD:15316', 'GARD:15357', 'GARD:15388', 'GARD:15394',
'GARD:15395', 'GARD:15401', 'GARD:15402', 'GARD:15403', 'GARD:15415',
'GARD:15422', 'GARD:15432', 'GARD:15443', 'GARD:15467', 'GARD:15483',
'GARD:15504', 'GARD:15513', 'GARD:15525', 'GARD:15555', 'GARD:15564',
'GARD:15565', 'GARD:15566', 'GARD:15567', 'GARD:15587', 'GARD:15600',
'GARD:15603', 'GARD:15604', 'GARD:15605', 'GARD:15606', 'GARD:15607',
'GARD:15608', 'GARD:15632', 'GARD:15637', 'GARD:15650', 'GARD:15651',
'GARD:15657', 'GARD:15659', 'GARD:15696', 'GARD:15697', 'GARD:15752',
'GARD:15779', 'GARD:15784', 'GARD:15785', 'GARD:15788', 'GARD:15848',
'GARD:15853', 'GARD:15854', 'GARD:15986', 'GARD:15992', 'GARD:16059',
'GARD:16131', 'GARD:16161', 'GARD:16184', 'GARD:16265', 'GARD:16267',
'GARD:16269', 'GARD:16334', 'GARD:16337', 'GARD:16823', 'GARD:17047',
'GARD:17343', 'GARD:17457', 'GARD:17458', 'GARD:17459', 'GARD:17460',
'GARD:17461', 'GARD:17462', 'GARD:17463', 'GARD:17464', 'GARD:17465',
'GARD:17514', 'GARD:17612', 'GARD:17795', 'GARD:17861', 'GARD:18046',
'GARD:18057', 'GARD:18059', 'GARD:18060', 'GARD:18061', 'GARD:18259',
'GARD:18285', 'GARD:18304', 'GARD:18384', 'GARD:18385', 'GARD:18472',
'GARD:18477', 'GARD:18479', 'GARD:18485', 'GARD:18486', 'GARD:18512',
'GARD:18550', 'GARD:18575', 'GARD:18577', 'GARD:18578', 'GARD:18579',
'GARD:18580', 'GARD:18581', 'GARD:18582', 'GARD:18594', 'GARD:18595',
'GARD:18596', 'GARD:18608', 'GARD:18609', 'GARD:18613', 'GARD:20322',
'GARD:21425', 'GARD:2162', 'GARD:21865', 'GARD:22318', 'GARD:22319',
'GARD:2456', 'GARD:3363', 'GARD:3364', 'GARD:3365', 'GARD:3366',
'GARD:3367', 'GARD:3368', 'GARD:9185'
]

# UMLS code blacklist exclusively used for the clinical trial database
umls_blacklist = [
"C4699604",
"C3843766",
"C5425801",
"C4699618",
"C5201140",
"C3843716",
"C2981698",
"C2827734",
"C3816745",
"C2981700",
"C2827735",
"C3833492",
"C3838680",
"C2981702",
"C2827736",
"C3838679",
"C5425955",
"C5425956",
"C3840862",
"C0205161",
"C1704258",
"C0001314",
"C0231176",
"C4061114",
"C5401128",
"C0277562",
"C1853562",
"C1863204",
"C0679246",
"C0854739",
"C4296616",
"C0678798",
"C0879626",
"C0877248",
"C0559546",
"C3811621",
"C0003944",
"C0243082",
"C0243083",
"C0521989",
"C1281905",
"C0236018",
"C0004368",
"C4076147",
"C0576806",
"C0005699",
"C1262869",
"C0005758",
"C3872897",
"C3810851",
"C4055223",
"C0221444",
"C4068804",
"C0009566",
"C2945640",
"C0750484",
"C0009676",
"C3842675",
"C3842674",
"C0439857",
"C0332155",
"C0277545",
"C0012634",
"C0338067",
"C0012691",
"C0580846",
"C0013687",
"C0233542",
"C0589120",
"C0332167",
"C4319571",
"C0850707",
"C2004062",
"C5452894",
"C0020875",
"C1397014",
"C0333124",
"C3834263",
"C4698019",
"C0021167",
"C5236002",
"C0549159",
"C1848924",
"C0439663",
"C3842782",
"C1881358",
"C0439044",
"C4520849",
"C5201228",
"C4745084",
"C3826020",
"C0025115",
"C0521848",
"C0746471",
"C1863621",
"C1863620",
"C1829822",
"C5551868",
"C5445274",
"C4740690",
"C4050513",
"C1834870",
"C3281089",
"C0549649",
"C4740691",
"C4085643",
"C5201148",
"C3840684",
"C4054479",
"C1837655",
"C0596988",
"C4531100",
"C1513916",
"C0205160",
"C0206736",
"C0027960",
"C0746890",
"C0497153",
"C1298908",
"C0441962",
"C3842236",
"C5452905",
"C0429524",
"C1536696",
"C0410000",
"C3854027",
"C0747122",
"C0332568",
"C0233071",
"C4554418",
"C2828386",
"C0549206",
"C4737703",
"C0701159",
"C1277626",
"C0450407",
"C0450408",
"C0450409",
"C4699164",
"C1837385",
"C1831741",
"C5453004",
"C0240795",
"C1514241",
"C1446409",
"C0332149",
"C0856742",
"C0241311",
"C0150312",
"C0460139",
"C0332148",
"C0231339",
"C0033213",
"C1545588",
"C3715209",
"C1838994",
"C0678236",
"C1514725",
"C2931829",
"C0392756",
"C0034951",
"C0424424",
"C0445223",
"C2826292",
"C1552052",
"C4554476",
"C0871269",
"C0035579",
"C0850664",
"C0035648",
"C5392186",
"C0035851",
"C3203359",
"C2362502",
"C5444316",
"C3244287",
"C0449820",
"C0235195",
"C5400562",
"C5445810",
"C0205082",
"C4740692",
"C0677946",
"C3840271",
"C0038187",
"C0333138",
"C1261287",
"C1760428",
"C0438696",
"C0038663",
"C0038661",
"C0332516",
"C1457887",
"C0039082",
"C5444317",
"C4314520",
"C1366940",
"C0205400",
"C4018905",
"C1839140",
"C2717979",
"C0000932",
"C3840880",
"C0087130",
"C5197901",
"C1408353",
"C0087136",
"C4698437",
"C0237284",
"C0443343",
"C1735591",
"C0917808",
"C4740675",
"C0442811",
"C4740674",
"C1855575",
"C0042693",
"C0376705",
"C1821973",
"C0235394",
"C3714552",
"C0043084",
"C0686747",
"C0686750",
"C0458075",
"C0686744",
"C0686751",
"C1286385",
"C1698590",
"C0393773",
"C0233481",
"C1457868",
"C4316810",
"C3845821"
]
