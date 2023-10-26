#!/usr/bin/env python3

# Copyright (C) 2023
# Petrakopoulos Vasilis
# EIDA Technical Committee @ National Observatory of Athens, Greece
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# This is a script used for removing WFCatalog entries with files that do not exist in both the EIDA FDSN station output and the node archive.
# The script reads these files from the table "remove_from_wfcatalog" of the "inconsistencies_results.db" SQLite database file,
# which is produced by executing the "check_consistency.py" script.
# Simply execute the script after ensuring that the mongo client -below import statements- is set according to your system.


import pymongo
import sqlite3
import os
import logging


# change the below according to your system
mongo_uri = os.getenv('WFCC_MONGO_URI', 'mongodb://localhost:27017')
client = pymongo.MongoClient(mongo_uri)
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO) # if desired modify this line to output logging details to a specified file


collection = client.wfrepo.daily_streams
# connect to database if exists
if os.path.exists(os.path.join(os.getcwd(), 'inconsistencies_results.db')):
    logging.info("Retrieving names of files to be removed from WFCatalog")
    conn = sqlite3.connect('inconsistencies_results.db')
    cursor = conn.cursor()
    # retrieve file names of files that need to be removed from WFCatalog
    file_ids = cursor.execute('SELECT fileName FROM remove_from_wfcatalog').fetchall()
    conn.commit()
    conn.close()

    logging.info("Removing files from WFCatalog")
    # remove WFCatalog entries
    for file_id in file_ids:
        collection.delete_one({'fileId': file_id[0]})


client.close()
