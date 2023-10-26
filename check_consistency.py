#!/bin/env python3

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
# This is a script used for finding inconsistencies between archive files, FDSN metadata and WFCatalog database.
# The script produces one inconsistencies_results.db SQLite database file with the following tables:
#  - inconsistent_metadata which includes the files that are "orphaned" (i.e. without any metadata)
#  - missing_in_wfcatalog which includes the files that are missing in WFCatalog database
#  - inconsistent_checksum which includes the files that have inconsistent checksum in WFCatalog database (file produced only if -c option specified)
#  - older_date which includes the files that have been modified after the date they were added in WFCatalog database
#  - remove_from_wfcatalog which includes the files that should be removed from wfcatalog (i.e. they are not in archive or are "orphaned")
#  - inappropriate_naming which includes the files that their naming does not follow the usual pattern of NET.STA.LOC.CHAN.NEL.YEAR.JDAY
# The script can take some arguments; look at parse_arguments function for more details or execute "./check_consistency.py -h" for help.
# Simply execute the script with the desired options AFTER either changing the paths and urls -just below import statements-
# or using environment variables according to your system.


import requests
import datetime
import hashlib
import logging
import argparse
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import pymongo


# change the below according to your system
mongo_uri = os.getenv('WFCC_MONGO_URI', 'mongodb://localhost:27017')
client = pymongo.MongoClient(mongo_uri)
archive_path = os.getenv('WFCC_ARCHIVE_PATH', '/data') # !!! use full path here
fdsn_endpoint = os.getenv('WFCC_FDSN_ENDPOINT', 'eida.gein.noa.gr')
fdsn_station_url = f"https://{fdsn_endpoint}/fdsnws/station/1/query?level=channel&format=text&nodata=404"
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO) # if desired modify this line to output logging details to a specified file


def parse_arguments():
    """
    Method to parse arguments to run the script for specified years and to exclude some networks
    """
    # default values for start and end time (last year)
    sy = datetime.datetime.now().year - 1
    ey = sy
    desc = 'Script to check inconsistencies between archive, metadata and WFCatalog database.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-s', '--start', default=sy, type=int,
                        help='Year to start the test (default=last year).')
    parser.add_argument('-e', '--end', default=ey, type=int,
                        help='Year to end the test (default=last year).')
    parser.add_argument('-x', '--exclude', default=None,
                        help='List of comma-separated networks to be excluded from this test (e.g. XX,YY,ZZ).')
    parser.add_argument('-c', '--checksum', action='store_true',
                        help='Check inconsistency of checksums in WFCatalog. Warning: this test takes a lot of time.')

    return parser.parse_args()


def getMD5Hash(f):
    """
    Method to generate md5 hashes used for the checksum field
    """
    try:
        BLOCKSIZE = 65536
        hasher = hashlib.md5()
        with open(f, "rb") as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
    except Exception as ex:
        logging.error(ex)
        return None

    return hasher.hexdigest()


def getFromDB():
    """
    Method to retrieve all files in WFCatalog database
    """
    collection = client.wfrepo.daily_streams
    # for entries with ts date between start and end and with net not included in the excluded networks
    # fetch the last file with its checksum and created date included in the files attribute
    # return a dictionary {name: (checksum, created)} for all entries
    query_result = list(collection.aggregate([
        {
            "$match": {
                "ts": {
                    "$gte": datetime.datetime(args.start, 1, 1),
                    "$lte": datetime.datetime(args.end, 12, 31, 23, 59, 59, 999999)
                },
                "net": {
                    "$nin": args.exclude.split(',') if args.exclude else []
                }
            }
        },
        {
            "$project": {
                "lastFile": { "$arrayElemAt": ["$files", -1] },
                "created": 1
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        "$lastFile",
                        {
                            "created": "$created"
                        }
                    ]
                }
            }
        },
    ]))
    client.close()

    return {r["name"]: (r["chksm"], r["created"]) for r in query_result}


def getFromFDSN():
    """
    Method to retrieve all metadata from FDSN station service
    """
    # return a nested dictionary in the form {network: {station: {location: {channel:[(epoch_start, epoch_end)]}}}}
    # example: nslce['HL']['ATH']['00']['HHN'] = [('2010-03-02T00:00:00'), ('')]
    fdsnResponse = requests.get(fdsn_station_url)
    linesList = fdsnResponse.text.splitlines()[1:]
    nslce = {}
    for line in linesList:
        parts = line.split('|')
        n, s, l, c, es, en = parts[0], parts[1], parts[2], parts[3], parts[-2], parts[-1]
        if n not in nslce:
            nslce[n] = {}
        if s not in nslce[n]:
            nslce[n][s] = {}
        if l not in nslce[n][s]:
            nslce[n][s][l] = {}
        if c not in nslce[n][s][l]:
            nslce[n][s][l][c] = []
        nslce[n][s][l][c].append((es, en))

    return nslce


def write_results():
    """
    Method to write results to new sqlite3 database
    """
    # remove previous database file if exists
    results_file = os.path.join(os.getcwd(), 'inconsistencies_results.db')
    if os.path.exists(results_file):
        os.remove(results_file)

    # create database and tables
    try:
        conn = sqlite3.connect('inconsistencies_results.db')
        cursor = conn.cursor()
        table_names = ['inconsistent_metadata', 'missing_in_wfcatalog', 'inconsistent_checksum', 'older_date', 'remove_from_wfcatalog', 'inappropriate_naming']
        for tn in table_names:
            cursor.execute(f'''
                CREATE TABLE {tn} (
                    net TEXT,
                    sta TEXT,
                    loc TEXT,
                    cha TEXT,
                    year INTEGER,
                    jday INTEGER,
                    fileName TEXT PRIMARY KEY
                )
                ''')
    except Exception as ex:
        logging.Error(ex)

    # insert to inconsistent_metadata
    data = []
    for item in inconsistent_epoch_files:
        parts = item.split('.')
        data.append((parts[0], parts[1], parts[2], parts[3], parts[5], parts[6], item))
    sql = 'INSERT INTO inconsistent_metadata (net, sta, loc, cha, year, jday, fileName) VALUES (?, ?, ?, ?, ?, ?, ?)'
    try:
        cursor.executemany(sql, data)
    except Exception as ex:
        logging.Error(ex)

    # insert to missing_in_wfcatalog
    data = []
    for item in missing_in_mongo_files:
        parts = item.split('.')
        data.append((parts[0], parts[1], parts[2], parts[3], parts[5], parts[6], item))
    sql = 'INSERT INTO missing_in_wfcatalog (net, sta, loc, cha, year, jday, fileName) VALUES (?, ?, ?, ?, ?, ?, ?)'
    try:
        cursor.executemany(sql, data)
    except Exception as ex:
        logging.Error(ex)

    # insert to inconsistent_checksum
    data = []
    for item in inconsistent_checksum_files:
        parts = item.split('.')
        data.append((parts[0], parts[1], parts[2], parts[3], parts[5], parts[6], item))
    sql = 'INSERT INTO inconsistent_checksum (net, sta, loc, cha, year, jday, fileName) VALUES (?, ?, ?, ?, ?, ?, ?)'
    try:
        cursor.executemany(sql, data)
    except Exception as ex:
        logging.Error(ex)

    # insert to older_date
    data = []
    for item in older_date_files:
        parts = item.split('.')
        data.append((parts[0], parts[1], parts[2], parts[3], parts[5], parts[6], item))
    sql = 'INSERT INTO older_date (net, sta, loc, cha, year, jday, fileName) VALUES (?, ?, ?, ?, ?, ?, ?)'
    try:
        cursor.executemany(sql, data)
    except Exception as ex:
        logging.Error(ex)

    # insert to remove_from_wfcatalog
    data = []
    for item in all_files_mongo:
        parts = item.split('.')
        data.append((parts[0], parts[1], parts[2], parts[3], parts[5], parts[6], item))
    sql = 'INSERT INTO remove_from_wfcatalog (net, sta, loc, cha, year, jday, fileName) VALUES (?, ?, ?, ?, ?, ?, ?)'
    try:
        cursor.executemany(sql, data)
    except Exception as ex:
        logging.Error(ex)

    # insert to inappropriate_naming
    data = []
    for item in inconsistent_file_naming:
        parts = item.split('.')
        try:
            data.append((parts[0], parts[1], parts[2], parts[3], parts[5], parts[6], item))
        except Exception as ex:
            data.append((None, None, None, None, None, None, item))
    sql = 'INSERT INTO inappropriate_naming (net, sta, loc, cha, year, jday, fileName) VALUES (?, ?, ?, ?, ?, ?, ?)'
    try:
        cursor.executemany(sql, data)
    except Exception as ex:
        logging.Error(ex)

    conn.commit()
    conn.close()


def process_file(file):
    """
    Method to process each file and append it to the appropriate list
    """
    # file is a full path string
    fileName = file.split('/')[-1]
    parts = fileName.split('.')
    try:
        # turn years and julian days to dates
        yearDay = datetime.datetime.strptime(parts[-2] + '.' + parts[-1], '%Y.%j').date()
    except:
        inconsistent_file_naming.append(fileName)
        return
    meta_OK = False
    # NOTE: files that correspond to a channel not in the location mentioned in the FDSN service output are excluded
    if parts[2] in nslce[file.split('/')[-4]][file.split('/')[-3]] and parts[3] in nslce[file.split('/')[-4]][file.split('/')[-3]][parts[2]]:
        # check if there is an FDSN epoch matching file name
        for epoch in nslce[network][station][parts[2]][parts[3]]:
            start = datetime.datetime.strptime(epoch[0].split('T')[0], '%Y-%m-%d').date()
            endEmpty = epoch[1] if epoch[1] else '2200-01-01T00:00:00'
            end = datetime.datetime.strptime(endEmpty.split('T')[0], '%Y-%m-%d').date()
            if start <= yearDay <= end:
                meta_OK = True
                break
        if not meta_OK:
            inconsistent_epoch_files.append(fileName)
        elif fileName in all_files_mongo:
            # check checksum consistency if asked
            if args.checksum and getMD5Hash(file) != all_files_mongo[fileName][0]:
                inconsistent_checksum_files.append(fileName)
            # check if file was added in WFCatalog before the last time it was modified
            if all_files_mongo[fileName][1] < datetime.datetime.fromtimestamp(os.path.getmtime(file)):
                older_date_files.append(fileName)
            # file is consistent with metadata and exists in WFCatalog
            # remove file so only files that should be removed from WFCatalog stay there
            del all_files_mongo[fileName]
        else:
            # file does not exist in WFCatalog whatsoever
            missing_in_mongo_files.append(fileName)


if __name__ == "__main__":
    args = parse_arguments()
    logging.info("Reading files from WFCatalog database")
    all_files_mongo = getFromDB()
    logging.info("Reading metadata from FDSN station service")
    nslce = getFromFDSN()

    # lists to put files according to them appearing as consistent or not between archive, metadata and WFCatalog database
    inconsistent_epoch_files = []
    inconsistent_file_naming = []
    missing_in_mongo_files = []
    inconsistent_checksum_files = []
    older_date_files = []

    # search archive and find files consistent or inconsistent with metadata and that exist or not or have inconsistent checksum in WFCatalog database
    logging.info("Start searching archive")
    allNets = list(nslce.keys()) # all networks of current node
    for year in os.listdir(archive_path):
        if args.start <= int(year) <= args.end:
            logging.info("Year "+year)
            for network in os.listdir(os.path.join(archive_path, year)):
                # ignore networks not in FDSN output or networks to be excluded
                if network in allNets and (not args.exclude or network not in args.exclude):
                    allStas = list(nslce[network].keys()) # all stations of current network
                    for station in os.listdir(os.path.join(archive_path, year, network)):
                        # ignore stations not in current network in FDSN output
                        if station in allStas:
                            # take all available channels from all available locations of station
                            allChanns = []
                            for location_data in nslce[network][station].values():
                                for chann in location_data.keys():
                                    allChanns.append(chann)
                            for channel in os.listdir(os.path.join(archive_path, year, network, station)):
                                # ignore channels not in FDSN output of current station
                                if channel.split('.')[0] in allChanns:
                                    ### NOTE: below uncomment either lines for single or multi core execution
                                    ### below 2 lines are for single core code execution
                                    #for file in os.listdir(os.path.join(archive_path, year, network, station, channel)):
                                        #process_file(os.path.join(archive_path, year, network, station, channel, file))
                                    ### below 4 lines are for multi-core code execution
                                    files = os.listdir(os.path.join(archive_path, year, network, station, channel))
                                    files = [os.path.join(archive_path, year, network, station, channel, f) for f in files]
                                    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                                        executor.map(process_file, files)

    logging.info("Writing results to SQLite database file")
    write_results()
