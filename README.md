# wfc-consistency
A project to provide some scripts to check WFCatalog database consistency with the node's archive, as well as take some actions regarding found inconsistencies.

The project consists of the following files:

#### check_consistency.py
This is a script used for finding inconsistencies between archive files, FDSN station metadata and WFCatalog database.

The script produces the following result files:
 - `inconsistent_metadata.txt`, which includes the files that are *orphaned* (i.e. without any metadata).
 - `missing_in_wfcatalog.txt`, which includes the files that are missing in WFCatalog database.
 - `inconsistent_checksum.txt`, which includes the files that have inconsistent checksum in WFCatalog database (file produced only if `-c` option specified).
 - `remove_from_wfcatalog.txt`, which includes the files that should be removed from wfcatalog (i.e. they are not in archive or are *orphaned*).
 - `inappropriate_naming.txt`, which includes the files that their naming does not follow the usual pattern of *NET.STA.LOC.CHAN.NEL.YEAR.JDAY*.

The script can be executed with some options:
 - `-h` or `--help` to print a help message.
 - `-s` or `--start` followed by a number for the year to start the test (default = last year).
 - `-e` or `--end` followed by a number for the year to end the test (default = last year).
 - `-x` or `--exclude` followed by a comma-separated list of networks to be excluded from this test (e.g. XX,YY,ZZ).
 - `-c` or `--checksum` to check inconsistency of checksums in WFCatalog. Warning: this test takes **too much** time.

Simply execute the script with the desired options **after** changing the paths and URLs just below import statements into the script according to your system.

#### delete_superfluous.py
This is a script used for removing WFCatalog entries with files that do not exist in both the EIDA FDSN station output and the node's archive.

The script reads these files from a file named `remove_from_wfcatalog.txt`, which is produced by executing the `check_consistency.py` script.

Simply execute the script **after** ensuring that the Mongo client -below import statements into the script- is set according to your system.

#### add_missing.py
Work in progress...

#### update_checksum.py
Work in progress...
