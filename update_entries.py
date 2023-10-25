#!/usr/bin/python3

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
# This is a script used for updating files in WFCatalog with inconsistent checksums or older creation date than last time modified in archive.
# The script reads these files from the tables "inconsistent_checksum" and "older_date" of the
# "inconsistencies_results.db" SQLite database file, which is produced by executing the "check_consistency.py" script.
# Simply execute the script AFTER ensuring that paths and collector options -below import statements- are set according to your system.


import sqlite3
import os
import json
import subprocess
import logging


# change the below according to your system
archive = '/darrays/fujidata-thiseio/archive/' # archive path
wfcConfigFile = '/home/sysop/Programs/wfcatalogue2023/wfcatalog/collector/config.json' # WFCatalog collector config.json file
wfcCollectorEnv = '/home/sysop/Programs/wfcatalogue2023/wfcatalog/collector/.env/bin/python' # WFCatalog collector virtual environment
wfcCollector = '/home/sysop/Programs/wfcatalogue2023/wfcatalog/collector/WFCatalogCollector.py' # WFCatalogCollector.py script
collectorOptions = ['--flags', '--csegs', '--update', '--force', '--dir', archive] # options to execute WFCatalogCollector.py script
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO) # if desired modify this line to output logging details to a specified file


# connect to SQLite database if exists
if os.path.exists(os.path.join(os.getcwd(), 'inconsistencies_results.db')):
    logging.info("Retrieving names of files to be updated in WFCatalog")
    conn = sqlite3.connect('inconsistencies_results.db')
    cursor = conn.cursor()
    # retrieve file names of files to be inserted into WFCatalog
    file_ids = cursor.execute('SELECT fileName FROM missing_in_wfcatalog UNION SELECT fileName FROM older_date').fetchall()
    conn.commit()
    conn.close()


# open config.json file of WFCatalog collector and add to the "WHITE" field the names of the files to be inserted in WFCatalog
logging.info("Write to config.json of WFCatalog collector")
with open(wfcConfigFile, 'r') as config_file:
    config = json.load(config_file)
old_white = config["FILTERS"]["WHITE"]
config["FILTERS"]["WHITE"] = [f[0] for f in file_ids]
with open(wfcConfigFile, 'w') as config_file:
    json.dump(config, config_file, indent=2)


# execute the WFCatalog collector
logging.info("Execute WFCatalog collector")
try:
    subprocess.run([wfcCollectorEnv, wfcCollector] + collectorOptions)
except KeyboardInterrupt:
    # this will enforce the undoing of changes in the config.json file in case of interrupt
    pass


# undo the changes in the config.json file
logging.info("Undo changes to config.json")
with open(wfcConfigFile, 'r') as config_file:
    config = json.load(config_file)
config["FILTERS"]["WHITE"] = old_white
with open(wfcConfigFile, 'w') as config_file:
    json.dump(config, config_file, indent=2)
