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
# This is a script used for adding entries to WFCatalog for files that are missing,
# although do exist in both the EIDA FDSN station output and the node archive.
# The script reads these files from the table "missing_in_wfcatalog" of the "inconsistencies_results.db" SQLite database file,
# which is produced by executing the "check_consistency.py" script.
# Simply execute the script AFTER ensuring that paths and collector options -below import statements- are set according to your system.


import sqlite3
import os
import json
import subprocess
import logging


# change the below according to your system
wfcCollectorDir = os.getenv('WFCC_COLLECTOR_DIR', '/home/Programs/wfcatalog/collector')
wfcCollectorEnv = f'{wfcCollectorDir}/.env/bin/python' # WFCatalog collector virtual environment
wfcCollector = f'{wfcCollectorDir}/WFCatalogCollector.py' # WFCatalogCollector.py script
collectorOptions = ['--flags', '--csegs', '--list'] # options to execute WFCatalogCollector.py script
batch_size = 500 # the collector script will be executed for batches of this size of files, otherwise bash command size limit might be exceeded
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO) # if desired modify this line to output logging details to a specified file


# connect to SQLite database if exists
if os.path.exists(os.path.join(os.getcwd(), 'inconsistencies_results.db')):
    logging.info("Retrieving names of files to be added in WFCatalog")
    conn = sqlite3.connect('inconsistencies_results.db')
    cursor = conn.cursor()
    # retrieve file names of files to be inserted into WFCatalog
    file_ids = cursor.execute('SELECT fileName FROM missing_in_wfcatalog').fetchall()
    conn.close()


# execute the WFCatalog collector in batches
for i in range(0, len(file_ids), batch_size):
    batch = [f[0] for f in file_ids[i:i+batch_size]]
    logging.info(f"Execute WFCatalog collector for batch {int(i/batch_size+1)}")
    subprocess.run([wfcCollectorEnv, wfcCollector] + collectorOptions + [json.dumps(batch)])
