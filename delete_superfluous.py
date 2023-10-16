#!/usr/bin/python3

"""
This is a script used for removing WFCatalog entries with files that do not exist in both the EIDA FDSN station output and the node archive.
The script reads these files from a file named "missing_in_wfcatalog.txt", which is produced by executing the "check_consistency.py" script.
Simply execute the script after ensuring that the mongo client -below import statements- is set according to your system.
"""

import pymongo

# change the below according to your system
client = pymongo.MongoClient(host='localhost', port=27017)

# get all files to be inserted into WFCatalog
with open('missing_in_wfcatalog.txt', 'r') as file:
    file_ids = file.read().splitlines()[1:]

collection = client.wfrepo.daily_streams
for file_id in file_ids:
    # CODE for insertion
    # PROBLEM: an entry has too many attributes and is complex, need guidance for this

client.close()
