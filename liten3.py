#!/usr/bin/env python
#Liten3 - Deduplication command line tool
#Author: Salvatore Dipietro (Initial Development by Noah Gift and Alfredo Deza)
#License: GPLv3 License
#Email: dipietro.salvatore [at] gmail dot com

__version__ = "3.0.0"
__date__ = "2017-07-06"


"""
Liten3 walks through a given path and creates a Checksum based on
the Sha512 library storing all the data in a Sqlite3 database.
You can run different kinds of reports once this has been done,
which gives you the ability to determine what files to delete.
"""


import sys
import os
import pathlib
import sqlite3
import time
import hashlib
from optparse import Option
from optparse import OptionParser
from progressbar import Bar, Percentage, ProgressBar


debug = False











from progressbar import Bar, Percentage, ProgressBar




class DbWork(object):
    """All the Database work happens here"""

    def __init__(self, dbfilename, debug=False):
        self.debug = debug
        self.database = dbfilename
        self.connection()
        self.conn.text_factory = str

    def setDebug(self, debug=False):
        self.debug = debug

    def connection(self):
        createtables = False
        if self.database != ':memory:':
            if not os.path.isfile(self.database) :  #Not exist
                createtables = True

        self.conn = sqlite3.connect(os.path.abspath(self.database), isolation_level='exclusive')
        self.conn.row_factory = sqlite3.Row
        self.c = self.conn.cursor()


        if createtables:
            self.createtables()

        if self.debug:
            print("Drop duplicates table.")
            self.c.execute('DELETE FROM duplicate;')
            self.c.execute('VACUUM ;')

    def createtables(self):
        self.createtable1 = 'CREATE TABLE files (filesid INTEGER primary key, path TEXT, bytes INTEGER, hashesid INTEGER, ' \
                            'isUpdated BOOLEAN DEFAULT TRUE , lastupdate DEFAULT CURRENT_TIMESTAMP , ' \
                            'FOREIGN KEY(hashesid) REFERENCES hashes(hashesid))'

        self.createtable2 = 'CREATE TABLE hashes (hashesid INTEGER primary key, hash TEXT, lastupdate DEFAULT CURRENT_TIMESTAMP)'

        self.createtable3 = 'CREATE TABLE duplicates (duplicatesid INTEGER primary key, hashesid INTEGER, filesid INTEGER,  ' \
                            'isUpdated BOOLEAN default TRUE , lastupdate default CURRENT_TIMESTAMP,   ' \
                            'FOREIGN KEY(filesid) REFERENCES files(filesid), FOREIGN KEY(hashesid) REFERENCES hashes(hashesid))'

        self.c.execute(self.createtable1)
        self.c.execute(self.createtable2)
        self.c.execute(self.createtable3)


    def setIsUpdated(self, path):
        """Set all the files to isUpdated to detect the deleted files"""
        self.c.execute("UPDATE files SET isUpdated='TRUE'")    # remove previous works not completed

        if path[-1] != '/': path += '/'
        path += '%'
        self.c.execute("UPDATE files SET isUpdated='FALSE' WHERE path LIKE ?", (path,))   # set FALSE to all the files in the FOLDER.
        self.c.execute("UPDATE duplicates SET isUpdated = 'FALSE' WHERE  "
                       "filesid IN (SELECT filesid FROM files WHERE path LIKE ?)", (path,)) # set FALSE to all the duplicate in the FOLDER.



    def insertFile(self, path, size, getHash=False):
        """Inserts the file information into the database"""
        isHashed = False
        record = self.searchFile(path).fetchone()
        if record == None:
            values = (path, size)
            if self.debug: print("DB INSERT file: "+str(path))
            command = "INSERT INTO files (path, bytes, hashesid, isUpdated, lastupdate) VALUES (?, ?, NULL, 'TRUE', datetime())"
            if self.debug: print(command, values)
            self.c.execute(command, values)
        else:
            # Check if need to be updated
            if record['bytes'] != size:
                if self.debug: print("DB UPDATE file (size): " + str(path))
                values = (size, record['filesid'])
                command = "UPDATE files SET bytes=?, isUpdated='TRUE', lastupdate=datetime() WHERE filesid=?"
            else:
                if self.debug: print("DB UPDATE file: " + str(path))
                values = (record['filesid'],)
                command = "UPDATE files SET isUpdated='TRUE' WHERE filesid=?"
            self.c.execute(command, values)

            if record['hash'] != None:
                self.addHash(path)
                isHashed = True

        if getHash == True and isHashed == False: self.addHash(path)  # get Hash



    def insertHash(self, path, hash):
        """Inserts the hash into the database"""
        record = self.searchHash(hash).fetchone()
        while record == None:
            self.c.execute("INSERT INTO hashes (hash, lastupdate) VALUES(?, datetime())", (hash,))
            record = self.searchHash(hash).fetchone()

        values = (record['hashesid'], path)
        command = "UPDATE files SET hashesid = ? WHERE path = ? "
        self.c.execute(command, values)


    def searchFile(self, path):
        values = (path,)
        command = "SELECT f.filesid as filesid, h.hashesid as hashesid, f.path as path, f.bytes as bytes, h.hash as hash  " \
                  "FROM files f LEFT JOIN  hashes h on  h.hashesid  = f.hashesid  WHERE f.path=?"
        return self.c.execute(command, values)

    def searchHash(self, hash):
        return self.c.execute("SELECT * FROM hashes WHERE hash = ? ", (hash,))


    def findDuplicates(self, paths, allhash=False):
        if isinstance(paths, str): paths = [paths]

        command2 = []
        for path in paths:
            path = path.replace("'", "''")
            if path[-1] != '/': path += '/'
            path += '%'
            command2.append("path LIKE '" + path + "'")
            command2.append("OR")
        del command2[-1]


        """ Get records with same SIZE"""
        if not allhash:
            command1 = ["SELECT  bytes, count(*) as c FROM files WHERE"]
            command3 = ["GROUP BY bytes HAVING c > 1  ORDER BY c DESC" ]
            command = command1 + command2 + command3                 # GET all FILES with SAME SIZE
            if self.debug: print(" ".join(command))
            allsamesizeresults = self.c.execute(" ".join(command)).fetchall()

            count = 0
            print("\n\n")
            print("Calculate Hash for the files with the same size. Number of files: %i" % (len(allsamesizeresults)))
            pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(allsamesizeresults)).start()

            #Calculate the HASH for FILES with same SIZE
            for r in allsamesizeresults:
                samesize = self.c.execute("SELECT path FROM files WHERE bytes = ?", (r['bytes'],)).fetchall()
                for r_i in samesize:
                    self.addHash(r_i['path'])

                count += 1
                pbar.update(count)
            pbar.finish()
            print("\n")

        """ Get records with same HASH"""
        allsamehashresults = self.c.execute("SELECT hashesid, count(*) as c FROM files GROUP BY hashesid HAVING c > 1 ORDER BY c DESC").fetchall()

        #ADD duplicate for FILES with same HASH
        for r in allsamehashresults:
            if self.debug: print("Search for all the files with the hash: "+str(r['hash']))
            values = (r['hashesid'],)
            command1 = ["SELECT f.filesid as filesid, h.hashesid as hashesid, f.path as path, f.bytes as bytes, h.hash as hash",
                      "FROM files f LEFT JOIN  hashes h on h.hashesid  = f.hashesid  WHERE "]
            command3 = ["AND","h.hashesid = ?"]
            command= command1 + command2 + command3
            if self.debug: print(" ".join(command))
            samehashresults = self.c.execute(" ".join(command), values).fetchall()

            for r_i in samehashresults:
                self.addDuplicate(r_i['hashesid'], r_i['filesid'])




    def addDuplicate(self, hashesid, filesid):
        """Add Duplicate Record"""
        record = self.getDuplicateByIDs(hashesid,filesid).fetchone()
        if record == None:  #No previous record
            values = (hashesid, filesid)
            command = "INSERT INTO duplicates (hashesid, filesid, isUpdated,  lastupdate) VALUES(?, ?, 'TRUE', datetime())"
            self.c.execute(command, values)

    def getDuplicateByIDs(self, hashesid, filesid):
        values = (hashesid, filesid)
        command = "SELECT * FROM duplicates WHERE hashesid = ? AND filesid = ?"
        results = self.c.execute(command, values)
        return results

    def getDuplicates(self, paths):
        if isinstance(paths, str): paths = [paths]

        command2 = []
        for path in paths:
            path = path.replace("'", "''")
            if path[-1] != '/': path += '/'
            path += '%'
            command2.append("files.path LIKE '" + path + "'")
            command2.append("OR")
        del command2[-1]



        command1 = ["SELECT * FROM duplicates LEFT JOIN hashes ON duplicates.hashesid = hashes.hashesid LEFT JOIN files ON duplicates.filesid = files.filesid WHERE"]
        command3 = ["AND", "duplicates.isUpdated = 'TRUE'", "GROUP BY duplicates.hashesid" ]
        command = command1 + command2 + command3                 # GET all FILES with SAME SIZE
        if self.debug: print(" ".join(command))
        return self.c.execute(" ".join(command))


    def getDuplicate(self, hashesid, paths):
        if isinstance(paths, str): paths = [paths]

        command2 = []
        for path in paths:
            path = path.replace("'", "''")
            if path[-1] != '/': path += '/'
            path += '%'
            command2.append("files.path LIKE '" + path + "'")
            command2.append("OR")
        del command2[-1]


        command1 = ["SELECT * FROM files LEFT JOIN duplicates on files.filesid = duplicates.filesid  WHERE"]
        command3 = ["AND", "files.filesid IN ( SELECT filesid FROM duplicates WHERE hashesid = ?) " ]
        command = command1 + command2 + command3                 # GET all FILES with SAME SIZE
        if self.debug: print(" ".join(command))
        return self.c.execute(" ".join(command), (hashesid,))


    def addHash(self, path):
        hash = self.getHash(path)
        self.insertHash(path, hash)
        return hash

    def getHash(self, path):
        if self.debug: print("Calculate SHA hash of the file" + str(path))
        readfile = open(path, 'rb', buffering=0).read()  # readfile = open(path).read(16384)
        return hashlib.sha512(readfile).hexdigest()


    def rmOldFiles(self, paths):
        """Delete all the files in the path with isUpdated TRUE"""
        if isinstance(paths, str): paths = [paths]

        command2 = []
        for path in paths:
            path = path.replace("'", "''")
            if path[-1] != '/': path += '/'
            path += '%'
            command2.append("path LIKE '" + path + "'")
            command2.append("OR")
        del command2[-1]

        command1 = ["SELECT f.filesid","FROM files f LEFT JOIN  hashes h on  h.hashesid  = f.hashesid", "WHERE"]
        command3 = ["AND", "f.isUpdated='FALSE'"]
        command = command1 + command2 + command3
        results = self.c.execute(" ".join(command)).fetchall()

        for r in results:
            values = (r['filesid'],)
            self.c.execute("DELETE FROM duplicates WHERE filesid = ?", values)   #DELETE DUPLICATE record
            self.c.execute("DELETE FROM files WHERE filesid = ?", values)   #DELETE FILE record

        self.c.execute("DELETE FROM duplicates WHERE isUpdated = 'FALSE'")  # DELETE DUPLICATE record with ISUPDATE = FALSE

    def cleanUnusedHashes(self):
        #TODO
        pass
        # if r['hashesid'] != None:
        #     self.c.execute("DELETE FROM hashes WHERE filesid = ?", values)    #DELETE HASH record

    def close(self):
        self.conn.commit()
        self.conn.close()


class Walk(object):
    """Takes charge of harvesting file data"""

    def __init__(self, path, DB, size=0):
        self.path = path
        self.size = size
        self.db = DB
        self.debug = False

    def setDebug(self, debug=False):
        self.debug=debug
        self.db.setDebug(self.debug)

    def findfiles(self, hashall=False):
        """Walks through and entire directory to create the checksums exporting the result at the end."""
        searched_files = 0
        print("\n")
        print("Search files in folder: %s." % (self.path))
        number_of_files = sum(len(filenames) for path, dirnames, filenames in os.walk(self.path))
        self.db.setIsUpdated(self.path)

        print("Number of files to scan : %s" % number_of_files)
        pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=number_of_files).start()
        for root, dir, files in os.walk(self.path):
            for f in files:
                searched_files += 1

                try:
                    absolute = os.path.join(root, f)
                    if os.path.isfile(absolute) and os.path.islink(absolute) == False:   # Avoid Symbolic links
                        size = os.path.getsize(absolute)
                        if self.debug: print("File %s: %s" % (str(absolute), str(size)))

                        pbar.update((searched_files - 1) + 1)
                        if size > self.size:

                            if self.debug: print("Added File %i %s (%s)" % (searched_files, absolute, str(size)))
                            self.db.insertFile(absolute, size, hashall)

                except IOError:
                    pass
                    pbar.finish()


class Report(object):

    def __init__(self, DB, full=False):
        """Builds a full or specific report"""
        self.full = full
        self.DB = DB
        self.debug = False

        self.duplicatedFilesCount = None
        self.duplicatedFilesList = None
        self.duplicatedFilesSize = None

        if self.full:
            self.fullreport()

    def setDebug(self, debug=False):
        self.debug=debug

    def fullreport(self,paths, export=None):
        """Returns all reports available"""
        self.reportCountDuplicates(paths)
        self.reportListDuplicates(paths, export)
        self.reportAnalysisDuplicates()


    def reportCountDuplicates(self, paths):
        self.duplicatedFilesCount = len(self.DB.getDuplicates(paths).fetchall())
        print("")
        print("--------------------------------------")
        print("Number of duplicated files are: %i" % (self.duplicatedFilesCount))

    def reportListDuplicates(self, paths, export=None):
        duplicatedFilesList = dict()
        duplicatedFilesSize = 0
        if export is not None:  f = open(export, 'w')
        print("")
        print("--------------------------------------")
        print("Duplicate files: ")
        for r in self.DB.getDuplicates(paths).fetchall():
            hash = self.DB.getDuplicate(r['hashesid'], paths).fetchall()
            if len(hash) > 1:
                files = []
                c = 0
                for d in hash:
                    c += 1
                    files.append('"'+str(d['path'])+'"')
                    if c > 0: duplicatedFilesSize += r['bytes']


                line = "%s\t%s\t%s" % (r['hash'], self.humanvalue(r['bytes']), " ".join(files))
                duplicatedFilesList[r['hash']] = { "hash":r['hash'], "files":files, "size":r['bytes'] }

                print(line)
                if export is not None:  f.write(line+"\n")

        if export is not None:  f.close()
        self.duplicatedFilesList = duplicatedFilesList
        self.duplicatedFilesSize = duplicatedFilesSize


    def reportAnalysisDuplicates(self):
        print("--------------------------------------")
        print("Liten3 Full Reporting")
        print("Duplicate files found: %i" % (self.duplicatedFilesCount))
        print("Total space wasted: %s " % (self.humanvalue(self.duplicatedFilesSize)))

        print("")
        print("To delete files, run liten3 in interactive mode: python liten3.py -i")



    def humanvalue(self, value):
        """returns a human value for a file in MegaBytes"""
        if value > 1024 * 1024 * 1024:
            return "%.2fGB" % (value / 1024 / 1024 / 1024 )
        if value > 1024 * 1024:
            return "%.2fMB" % (value / 1024 / 1024)
        if value > 1024:
            return "%.2fKB" % (value / 1024 )
        if value > 0:
            return "%.2fB" % (value)
        else:
            return "0"



class Interactive(object):
    """This mode creates a session to delete files"""

    def __init__(self,DB, dryrun=False):
        self.DB = DB
        self.dryrun = dryrun
        self.debug = False
        self.autoDelete = False


    def setDebug(self, debug=False):
        self.debug=debug

    def session(self, paths="%"):
        "starts a new session"

        if self.dryrun:
            print("\n#####################################################")
            print(  "# Running in DRY RUN mode. No files will be deleted #")
            print(  "#####################################################\n")
        print("""
\t LITEN 3 \n

Starting a new Interactive Session.

* Duplicate files will be presented in numbered groups.
* Type one number at a time
* Hit Enter to skip to the next group.
* Ctrl-C cancels the operation, nothing will be deleted.
* Confirm your selections at the end.\n
-------------------------------------------------------\n""")

        for_deletion = list()

        try:
            for r in self.DB.getDuplicates(paths).fetchall():
                hash = self.DB.getDuplicate(r['hashesid'], paths).fetchall()
                if len(hash) > 1:
                    filepaths = list([""])
                    count = 1
                    for i in hash :
                        filepaths.append(i['path'])
                        if not self.autoDelete:
                            print("%d \t %s" % (count, i['path']))
                            count += 1
                    if self.autoDelete :
                        files = self.areFilesInFolder(filepaths)
                        if files is not None:
                            for_deletion = for_deletion + files

                if not self.autoDelete:
                    try:
                        answer = True
                        while answer:
                            choose = int(input("Choose a number to delete (Enter to skip): "))
                            if filepaths[choose] not in for_deletion:
                                for_deletion.append(filepaths[choose])
                            if not choose:
                                answer = False

                    except ValueError:
                        print("--------------------------------------------------\n")

            print("Files selected for complete removal:\n")
            for selection in for_deletion:
                if selection:
                    print(selection)
            print("\n")

            if self.dryrun:
                print("###########################")
                print("# DRY RUN mode ends here. #")
                print("###########################\n")

            if not self.dryrun:
                confirm = input("Type Yes to confirm (No to cancel): ")
                if confirm in ["Yes", "yes", "Y", "y"]:
                    for selection in for_deletion:
                        if selection:
                            try:
                                print("Removing file:\t %s" % selection)
                                os.remove(selection)
                            except OSError:
                                "Could not delete:\t %s \nCheck Permissions" % selection
            else:
                print("Cancelling operation, no files were deleted.")

        except KeyboardInterrupt:
            print("\nExiting nicely from interactive mode. No files deleted\n")


    def delete(self, deletePaths):
        self.autoDelete = True
        self.deletePaths = deletePaths

    def areFilesInFolder(self, filesPath):
        founded = list()
        toDelete = list()

        for file in filesPath:
            if file != "": #not empty content
                foundedInFolders = list()
                for folder in self.deletePaths:
                    if file.startswith(folder):
                        toDelete.append(file)
                        foundedInFolders.append(True)
                    else:
                        foundedInFolders.append(False)

                if True in foundedInFolders:
                    founded.append(True)
                else:
                    founded.append(False)

        #If there is a FALSE at least one copy remains in the system
        if False in founded:
            return toDelete
        else:
            return None






# URL https://stackoverflow.com/questions/4109436/processing-multiple-values-for-one-single-option-using-getopt-optparse
class MultipleOption(Option):
    ACTIONS = Option.ACTIONS + ("extend",)
    STORE_ACTIONS = Option.STORE_ACTIONS + ("extend",)
    TYPED_ACTIONS = Option.TYPED_ACTIONS + ("extend",)
    ALWAYS_TYPED_ACTIONS = Option.ALWAYS_TYPED_ACTIONS + ("extend",)

    def take_action(self, action, dest, opt, value, values, parser):
        if action == "extend":
            values.ensure_value(dest, []).append(value)
        else:
            Option.take_action(self, action, dest, opt, value, values, parser)





def main():
    """Parse the options"""

    p = OptionParser(option_class=MultipleOption,  usage='usage: %prog [OPTIONS]')

    p.add_option('--open', '-o', help="Use a specific database")
    p.add_option('--tmpDB', '-t', action="store_true", help="Use a temporary in-memory database")
    p.add_option('--path', '-p', action="extend", type="string", help="Supply a path to search")
    p.add_option('--delete', '-x', action="extend", type="string", help="Delete all the duplicates files in the provided path")
    p.add_option('--size', '-s', help="Search by Size in MB. Defaults to 0")
    p.add_option('--export', '-e', help="Export the list of duplicate file into text file")
    p.add_option('--hashall','-a', action="store_true",help="Hash all the files. By defauls it hash only files with the same size.")
    p.add_option('--dryrun', '-d', action="store_true", help="Does not delete anything. Use ONLY with interactive mode")
    p.add_option('--report', '-r', action="store_true", help="Generates a report from a previous run")
    p.add_option('--interactive', '-i', action="store_true", help='Interactive mode to delete files')
    options, arguments = p.parse_args()



    """ Test options"""
    if options.open and options.tmpDB:
        sys.stderr.write("Do you want to use a temporary database (--tmpDB) or not?")
        sys.exit(1)



    """ Setting  """
    #DB
    dbfilename = str(pathlib.Path.home()) + "/.liten3.sqlite"
    if options.open:  dbfilename = os.path.abspath(options.open)
    if options.tmpDB: dbfilename = ':memory:'
    DB = DbWork(dbfilename)
    DB.setDebug(debug)

    #SIZE
    mb=0
    if options.size:  mb = int(options.size) * 1048576

    #PATHS
    if options.path and len(options.path) > 0:
        absolutePath = list()
        for i in options.path:
            absolutePath.append(os.path.abspath(i))
        options.path = absolutePath

    if options.delete and len(options.delete) > 0:
        absolutePath = list()
        for i in options.delete:
            absolutePath.append(os.path.abspath(i))
        options.delete = absolutePath



    """ Start program"""
    if options.path:
        for path in options.path:
            if not os.path.isdir(path):
                sys.stderr.write("Search path does not exist or is not a directory: %s\n" % path)
                sys.exit(1)

        for path in options.path:
            try:
                run = Walk(path, DB, size=mb)
                run.setDebug(debug)
                run.findfiles(options.hashall)

            except (KeyboardInterrupt, SystemExit):
                print("\nExiting nicely from Liten3...")
                sys.exit(1)

        DB.rmOldFiles(options.path)
        DB.findDuplicates(options.path, options.hashall)
        options.report = options.path

    if options.report:
        out = Report(DB)
        if not options.path: options.path = "/"
        if options.export:
            out.fullreport(options.path, options.export)
        else:
            out.fullreport(options.path, None)


    if options.interactive or options.delete:
        if options.dryrun:
            run = Interactive(DB, dryrun=True)
        else:
            run = Interactive(DB)
        run.setDebug(debug)

        if options.delete: run.delete(options.delete)
        if not options.path:
            run.session("%")
        else:
            run.session(options.path)



    """ EXIT """
    DB.close()
    sys.exit(0)


if __name__=="__main__":
    main()
