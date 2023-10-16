#!/usr/bin/python3

"""
This is a script used for finding inconsistencies between archive files, FDSN metadata and WFCatalog database.
The script produces the following result files:
 - inconsistent_metadata.txt which includes the files that are "orphaned" (i.e. without any metadata)
 - missing_in_wfcatalog.txt which includes the files that are missing in WFCatalog database
 - inconsistent_checksum.txt which includes the files that have inconsistent checksum in WFCatalog database (file produced only if -c option specified)
 - remove_from_wfcatalog.txt which includes the files that should be removed from wfcatalog (i.e. they are not in archive or are "orphaned")
 - inappropriate_naming.txt which includes the files that their naming does not follow the usual pattern of NET.STA.LOC.CHAN.NEL.YEAR.JDAY
The script can take some arguments; look at parse_arguments function for more details or execute "./check_consistency.py -h" for help.
Simply execute the script with the desired arguments AFTER changing the paths and urls just below import statements according to your system.
"""

import urllib.request
import requests
import datetime
import hashlib
import logging
import argparse
import os
import sys
import pymongo
from concurrent.futures import ThreadPoolExecutor


# change the below according to your system
client = pymongo.MongoClient(host='localhost', port=27017)
archive_path = '/darrays/fujidata-thiseio/archive/' # !!! use full path here
fdsn_station_url = 'https://eida.gein.noa.gr/fdsnws/station/1/query?level=channel&format=text&nodata=404'


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
    # fetch the last file and its checksum included in the files attribute
    # return a dictionary {name: checksum} for all entries
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
                "lastFile": { "$arrayElemAt": ["$files", -1] }
            }
        },
        {
            "$replaceRoot": { "newRoot": "$lastFile" }
        },
    ]))
    client.close()

    return {r["name"]: r["chksm"] for r in query_result}


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
            if args.checksum and getMD5Hash(file) != all_files_mongo[fileName]:
                inconsistent_checksum.append(fileName)
            else:
                # file is consistent with metadata and exists in WFCatalog
                archive_files_ok.add(fileName)
            # remove file so only files that should be removed from WFCatalog stay there
            del all_files_mongo[fileName]
        else:
            # file does not exist in WFCatalog whatsoever
            missing_in_mongo_files.append(fileName)


if __name__ == "__main__":
    args = parse_arguments()
    all_files_mongo = getFromDB()
    nslce = getFromFDSN()

    # lists to put files according to them appearing as consistent or not between archive, metadata and WFCatalog database
    inconsistent_epoch_files = []
    inconsistent_file_naming = []
    archive_files_ok = set()
    missing_in_mongo_files = []
    inconsistent_checksum = []

    # search archive and find files consistent or inconsistent with metadata and that exist or not or have inconsistent checksum in WFCatalog database
    allNets = list(nslce.keys()) # all networks of current node
    for year in os.listdir(archive_path):
        if args.start <= int(year) <= args.end:
            logging.info(year) # TEST
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

    # write results to files
    with open("inconsistent_metadata.txt", "w") as file:
        file.write("Files without metadata (i.e. no epoch matching)\n")
        for item in inconsistent_epoch_files:
            file.write(item + "\n")

    with open("missing_in_wfcatalog.txt", "w") as file:
        file.write("Files missing from WFCatalog database\n")
        for item in missing_in_mongo_files:
            file.write(item + "\n")

    if args.checksum:
        with open("inconsistent_checksum.txt", "w") as file:
            file.write("Files with inconsistent checksum in WFCatalog database\n")
            for item in inconsistent_checksum:
                file.write(item + "\n")

    with open("remove_from_wfcatalog.txt", "w") as file:
        file.write("Files that should be removed from WFCatalog database\n")
        for item in all_files_mongo:
            file.write(item + "\n")

    with open("inappropriate_naming.txt", "w") as file:
        file.write("Files with inappropriate naming\n")
        for item in inconsistent_file_naming:
            file.write(item + "\n")
