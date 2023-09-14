import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import sysvars
import grant.init
from time import sleep

def main (update_from):
    grant.init.main()

