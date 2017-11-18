#################
#    LITEN 3    #
#################

Author: Salvatore Dipietro
Version: 3.0.0
Date: Dec 2017
Contact: dipietro.salvatore at gmail.com
License: GPLv3 

Liten3 will search a given directory and find duplicate files building a report at the end.

It uses an SQLite DB to store searches and optimize file handling after it has run.

SHA-512 is used as a checksum method, this will provide a better and more precise way of handling differences in files.
Currently working on GNU/Linux and Mac OS Operating Systems (not tested on Windows).


Dependencies
-----
This was developed with Python 3 and has not been tested with earlier versions of Python.
Liten3 depends on SQLite3. This will not be a problem in a normal Python install.
No installation is currently necessary, just uncompress the TAR file and run the file with Python.

If Python 3 is not installed on the system, run:
for Debian/Ubuntu:
    sudo apt install python3 python3-progressbar


Installation
------------
However, it will not work with earlier versions of Python because we are using functionality found in Python3
Download and Uncompress

tar xzvf Liten3-*.tar.gz


First Time Run
--------------
For the first time run you need to supply a path to search with the "-p" flag:

python liten3.py -p /path/to/directory

Liten3 creates an SQL file where all the files and hashes are inserted. To speed up the research and hashes of previously scanned folders, Liten3 no longer creates a new database each time it is executed but it updates the existing one.
In this way, the database stores all the files of the folders that it has been scanned in the past and it is able to identify duplicates on other folders that are not included in the scanning folder.
By default the SQL file is created in the home directory (~/.liten3.sqlite). It is possible to change the path or create multiple databases, using the option "-o sqliteFilename"
In the end of its execution, Liten3 shows the Report on the duplicated files.
If you want to save the list of duplicated files in an external file, run Liten3 with "-e filename":

python liten3.py -p /path/to/directory -e  filename

By default Liten3 hashes only the files of the same size in the searched folder. If you want you can force to hash all the files using the option "-a"

python liten3.py -a -p /path/to/directory


Choose File Size to Search
--------------------------
By default, Liten3 will search for all files. You can specify to analyse only files larger (in size) than a threshold (always in megabytes) with the "-s" flag:

python Liten3.py -s 5 -p /path/to/directory

The command above will search for files over 5 Megabytes in size in the given directory path.


Interactive Delete Session
--------------------------
Liten3 has an interactive session (you need to have run Liten3.py before or specify in the command) that will group identical files together and will let you choose the files to delete:

python Liten3.py -i

When interactive mode, you can hit Ctrl-C to quit (nothing will be deleted). The group of identical files will be numbered and you will be asked to type a number to delete or hit Enter to skip to the next group.


Dry Run
-------
A Dry Run option is available only when running the interactive mode. Nothing will be deleted when this option is used. Identical files will be shown and your selections will be saved. At the end of the session, a message will display that nothing was deleted. To use this option:

python Liten3.py -i -d

