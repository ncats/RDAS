import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append('/home/aom2/RDAS')
import sysvars
import grant.init
from time import sleep

def main ():
    grant.init.main()

