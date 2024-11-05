import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append('/home/aom2/RDAS')
import sysvars
import RDAS_GFKG.init
from time import sleep

def start_update ():
    RDAS_GFKG.init.main()

