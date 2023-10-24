# wfc-consistency
A project to provide some scripts to check WFCatalog database consistency with the node's archive, as well as take some actions regarding found inconsistencies.

The project consists of the following files:

#### check_consistency.py
This is a script used for finding inconsistencies between archive files, FDSN station metadata and WFCatalog database.

The script produces one `inconsistencies_results.db` SQLite database file with the following tables:
 - `inconsistent_metadata`, which includes the files that are *orphaned* (i.e. without any metadata).
 - `missing_in_wfcatalog`, which includes the files that are missing in WFCatalog database.
 - `inconsistent_checksum`, which includes the files that have inconsistent checksum in WFCatalog database (file produced only if `-c` option specified).
 - `older_date`, which includes the files that have been modified after the date they were added in WFCatalog database.
 - `remove_from_wfcatalog`, which includes the files that should be removed from wfcatalog (i.e. they are not in archive or are *orphaned*).
 - `inappropriate_naming`, which includes the files that their naming does not follow the usual pattern of *NET.STA.LOC.CHAN.NEL.YEAR.JDAY*.

The schema of all the above tables is the following:
| net | sta | loc | cha | year | jday | fileName
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| Network code (text) | Station code (text) | Location code (text) | Channel name (text) | Year (integer) | Julian day (integer) | File name (text)

The script can be executed with some options:
 - `-h` or `--help` to print a help message.
 - `-s` or `--start` followed by a number for the year to start the test (default = last year).
 - `-e` or `--end` followed by a number for the year to end the test (default = last year).
 - `-x` or `--exclude` followed by a comma-separated list of networks to be excluded from this test (e.g. XX,YY,ZZ).
 - `-c` or `--checksum` to check inconsistency of checksums in WFCatalog. Warning: this test takes **too much** time.

Simply execute the script with the desired options **after** changing the paths and URLs just below import statements into the script according to your system.

For example, the below line will execute the script to find inconsistencies from the beginning of 2010 until the end of 2022.

```
./check_consistency.py -s 2010 -e 2022
```

#### delete_superfluous.py
This is a script used for removing WFCatalog entries with files that do not exist in both the EIDA FDSN station output and the node's archive.

The script reads these files from from the table `remove_from_wfcatalog` of the `inconsistencies_results.db` SQLite database file, which is produced by executing the `check_consistency.py` script.

Simply execute the script **after** ensuring that the Mongo client -below import statements into the script- is set according to your system.

#### add_missing.py
Work in progress...

#### update_entries.py
Work in progress...
