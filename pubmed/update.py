import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import sysvars
import pubmed.init
from time import sleep
from gard.methods import get_node_counts

def main (update_from):
    pubmed.init.main(update_from=update_from)

main('11/09/23') #TEST
get_node_counts() #TEST
