import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from subprocess import *

#Script that populates required environment variables for running the RDAS system
#ENTIRE SCRIPT MUST BE RUN WITH SUDO
#BEFORE RUNNING SCRIPT MAKE SURE YOU HAVE YOUR CONDA ENVIROMENT ENABLED

def set_env (var, val):
    p = Popen(['conda', 'env', 'config', 'vars', 'set', f'{var}=\"{val}\"'], encoding='utf8')
    p.wait()

#Populate required enviroment values before running script
env = {
    'NEO4J_URI': None,
    'NEO4J_USERNAME': None,
    'NEO4J_PASSWORD': 'test',
    'AWS_ACCESS_KEY_ID': None,
    'AWS_SECRET_ACCESS_KEY': None,
    'AWS_SESSION_TOKEN': None,
    'PALANTIR_KEY': None,
    'METAMAP_KEY': None,
    'METAMAP_EMAIL': None,
    'OMIM_KEY': None,
    'NCBI_KEY': None
}

for k,v in env.items():
    if v == None:
        continue
    else:
        set_env(k,v)
