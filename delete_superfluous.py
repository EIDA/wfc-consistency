#!/usr/bin/python3

# Copyright (C) 2023
# Petrakopoulos Vasilis
# EIDA Technical Committee @ National Observatory of Athens, Greece
#
# This script is free software: you can redistribute it and/or modify it.
#
# This script is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY.

# This is a script used for removing WFCatalog entries with files that do not exist in both the EIDA FDSN station output and the node archive.
# The script reads these files from the table "remove_from_wfcatalog" of the "inconsistencies_results.db" SQLite database file,
# which is produced by executing the "check_consistency.py" script.
# Simply execute the script after ensuring that the mongo client -below import statements- is set according to your system.

import pymongo
import sqlite3
import os

# change the below according to your system
client = pymongo.MongoClient(host='localhost', port=27017)

collection = client.wfrepo.daily_streams
# connect to database if exists
if os.path.exists(os.path.join(os.getcwd(), 'inconsistencies_results.db')):
    conn = sqlite3.connect('inconsistencies_results.db')
    cursor = conn.cursor()
    # retrieve file names of files that need to be removed from WFCatalog
    file_ids = cursor.execute('SELECT fileName FROM remove_from_wfcatalog').fetchall()
    conn.commit()
    conn.close()

    # remove WFCatalog entries
    for file_id in file_ids:
        collection.delete_one({'fileId': file_id[0]})

client.close()
