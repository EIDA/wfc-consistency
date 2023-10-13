#!/usr/bin/python3

"""
This is a script used for finding incosistencies between archive files, FDSN metadata and WFCatalog database.
The script prints the files that:
 - are "orphaned" (i.e. without any metadata)
 - are missing in WFCatalog database
 - have inconsistent checksum in WFCatalog database
 - should be removed from wfcatalog (i.e. they are not in archive or are "orphaned")
The script can take some arguments, look at parse_arguments function.
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
    desc = 'Script to check incosistencies between archive, metadata and WFCatalog database.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-s', '--start', default=sy, type=int,
                        help='Year to start the test (default=last year).')
    parser.add_argument('-e', '--end', default=ey, type=int,
                        help='Year to end the test (default=last year).')
    parser.add_argument('-x', '--exclude', default=None,
                        help='List of comma-separated networks to be excluded from this test (e.g. XX,YY,ZZ).')

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
        incosistent_file_naming.append(fileName)
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
            incosistent_epoch_files.append(fileName)
        elif fileName in all_files_mongo:
            # check checksum consistency
            if getMD5Hash(file) != all_files_mongo[fileName]:
                incosistent_checksum.append(fileName)
            else:
                # file is consistent with metadata and exists in WFCatalog with consistent checksum
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
    incosistent_epoch_files = []
    incosistent_file_naming = []
    archive_files_ok = set()
    missing_in_mongo_files = []
    incosistent_checksum = []

    # search archive and find files consistent or inconsistent with metadata and that exist or not or have incosistent checksum in WFCatalog database
    allNets = list(nslce.keys()) # all networks of current node
    for year in os.listdir(archive_path):
        if args.start <= int(year) <= args.end:
            print(year) # TEST
            for network in os.listdir(os.path.join(archive_path, year)):
                # ignore networks not in FDSN output or networks to be excluded
                if network in allNets and (not args.exclude or network not in args.exclude):
                    print(network) # TEST
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
                                    # NOTE: below uncomment either lines for single or multi core execution
                                    # below 2 lines are for single core code execution
                                    #for file in os.listdir(os.path.join(archive_path, year, network, station, channel)):
                                        #process_file(os.path.join(archive_path, year, network, station, channel, file))
                                    # below 4 lines are for multi-core code execution
                                    files = os.listdir(os.path.join(archive_path, year, network, station, channel))
                                    files = [os.path.join(archive_path, year, network, station, channel, f) for f in files]
                                    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                                        executor.map(process_file, files)

    # print results
    print("Files without metadata (i.e. no epoch matching):")
    print(len(incosistent_epoch_files))
    print(incosistent_epoch_files[:10])

    print("Files missing from WFCatalog database:")
    print(len(missing_in_mongo_files))
    print(missing_in_mongo_files[:10])

    print("Files with incosistent checksum in WFCatalog database:")
    print(len(incosistent_checksum))
    print(incosistent_checksum[:10])

    print("Files that should be removed from WFCatalog database:")
    print(len(all_files_mongo))
    print(list(all_files_mongo.keys())[:10])

    print("Files with inappropriate naming:")
    print(len(incosistent_file_naming))
    print(incosistent_file_naming[:10])
