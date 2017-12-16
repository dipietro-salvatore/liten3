#!/usr/bin/env python
#Liten3 - Deduplication command line tool
#Author: Salvatore Dipietro (Initial Development by Noah Gift and Alfredo Deza)
#License: GPLv3 License
#Email: dipietro.salvatore [at] gmail dot com

__version__ = "3.0.1"
__date__ = "2017-11-20"


"""
Liten3 walks through a given path and creates a Checksum based on
the Sha512 library storing all the data in a Sqlite3 database.
You can run different kinds of reports once this has been done,
which gives you the ability to determine what files to delete.
"""


import sys, multiprocessing, os,pathlib
import sqlite3, time, hashlib
from optparse import Option, OptionParser
from progressbar import Bar, Percentage, ProgressBar
from multiprocessing.pool import ThreadPool
from multiprocessing import Queue, cpu_count
import hashlib


debug = False






################# LINE40 #############################








class Db(object):
    """ All the Database work happens here """

    def __init__(self, dbfilename, threads, debug=False):
        self.debug = debug
        self.database = dbfilename
        self.connection()
        self.threads = threads
        self.hashesClass = None
        self.cacheFilesClass = None
        self.cacheHashesClass = None

    def connection(self):
        createtables = False
        if self.database != ':memory:':
            if not os.path.isfile(self.database):  # Not exist
                createtables = True

        self.conn = sqlite3.connect(os.path.abspath(self.database), isolation_level='exclusive')
        self.conn.row_factory = sqlite3.Row
        self.c = self.conn.cursor()

        if createtables:
            self.createTables()

        if self.debug:
            print("Drop duplicates table.")
            self.execQuery('DELETE FROM duplicate;')
            self.execQuery('VACUUM ;')

    def createTables(self):
        self.createtable1 = 'CREATE TABLE files (filesid INTEGER primary key, path TEXT, bytes INTEGER, hashesid INTEGER, ' \
                            'isUpdated BOOLEAN DEFAULT TRUE , lastupdate DEFAULT CURRENT_TIMESTAMP , ' \
                            'FOREIGN KEY(hashesid) REFERENCES hashes(hashesid))'

        self.createtable2 = 'CREATE TABLE hashes (hashesid INTEGER primary key, hash TEXT, lastupdate DEFAULT CURRENT_TIMESTAMP)'

        self.createtable3 = 'CREATE TABLE duplicates (duplicatesid INTEGER primary key, hashesid INTEGER, filesid INTEGER,  ' \
                            'isUpdated BOOLEAN default TRUE , lastupdate default CURRENT_TIMESTAMP,   ' \
                            'FOREIGN KEY(filesid) REFERENCES files(filesid), FOREIGN KEY(hashesid) REFERENCES hashes(hashesid))'

        self.execQuery(self.createtable1)
        self.execQuery(self.createtable2)
        self.execQuery(self.createtable3)

        self.commit()

    def arePathsWildcard(self, paths):
        """ Check if a list of paths have the wildcard in the end of it (/%). If not it will add it."""
        if not isinstance(paths, list): paths = [paths]
        for i in range(len(paths)):
            paths[i] = self.isPathWildcard(str(paths[i]))

        return paths

    def isPathWildcard(self, path):
        path = path.replace("'", "''")

        if path[-2:] == "/%": return path
        if path[-1] != '/':  path += '/'
        path += '%'
        return path

    def setIsUpdated(self, path):
        """Set all the files to isUpdated to detect the deleted files"""
        self.execQuery("UPDATE files SET isUpdated='TRUE'")  # remove previous works not completed

        path = self.isPathWildcard(path)
        self.execQuery("UPDATE files SET isUpdated='FALSE' WHERE path LIKE ?",(path,))  # set FALSE to all the files in the FOLDER.
        self.execQuery("UPDATE duplicates SET isUpdated = 'FALSE' WHERE filesid IN (SELECT filesid FROM files WHERE path LIKE ?)",(path,))  # set FALSE to all the duplicate in the FOLDER.

    # USED?
    # def insertFile(self, path, size, getHash=False):
    #     """Inserts the file information into the database"""
    #     isHashed = getHash
    #
    #     record = self.searchFile(path).fetchone()
    #     if record == None:
    #         values = (path, size)
    #         if self.debug: print("DB INSERT file: " + str(path))
    #         command = "INSERT INTO files (path, bytes, hashesid, isUpdated, lastupdate) VALUES (?, ?, NULL, 'TRUE', datetime())"
    #         if self.debug: print(command, values)
    #         self.execQuery(command, values)
    #     else:
    #         # Check if need to be updated
    #         if record['bytes'] != size:
    #             if self.debug: print("DB UPDATE file (size): " + str(path))
    #             values = (size, record['filesid'])
    #             command = "UPDATE files SET bytes=?, isUpdated='TRUE', lastupdate=datetime() WHERE filesid=?"
    #         else:
    #             if self.debug: print("DB UPDATE file: " + str(path))
    #             values = (record['filesid'],)
    #             command = "UPDATE files SET isUpdated='TRUE' WHERE filesid=?"
    #         self.execQuery(command, values)
    #
    #         if record['hash'] != None:
    #             self.addHash(path)
    #             isHashed = True
    #
    #     if getHash == True and isHashed == False: self.addHash(path)  # get Hash


    def getFilesInPaths(self, paths):

        paths = self.arePathsWildcard(paths)
        if paths is None:
            q = "SELECT filesid,path,bytes,hashesid FROM files"
        else:
            q = "SELECT filesid,path,bytes,hashesid FROM files WHERE " + " ".join(self.getFilePathsQueryList(paths))

        return self.execQuery(q)


    def fillCacheFilesInPaths(self, paths):
        allFiles = self.getFilesInPaths(paths).fetchall()
        for file in allFiles:
            self.cacheFilesClass.add(file['path'], file['filesid'], file['bytes'], file['hashesid'])


    def insertFiles(self, pathFolder, filesDetailQueue, getHash=False, toprint=False):
        """Inserts the files information into the database"""
        insertQueryValuesList = list()
        updateQueryValuesList = list()

        while not filesDetailQueue.empty():
            file = filesDetailQueue.get()
            if file.path in self.cacheFilesClass.files:
                #Update
                value = (file.size, self.cacheFilesClass.getFilesid(file.path),)
                updateQueryValuesList.append( value )
                if not self.cacheFilesClass.sameSize(file.path, file.size) and self.cacheFilesClass.getHashesid(file.path) != None:
                    self.addHash(file.path)
            else:
                # Insert
                value = (file.path, file.size,)
                insertQueryValuesList.append( value )
                if getHash: self.addHash(file.path)


        insertQuery = "INSERT INTO files (path, bytes, hashesid, isUpdated, lastupdate) VALUES (?, ?, NULL, 'TRUE', datetime())"
        updateQuery = "UPDATE files SET bytes=?, isUpdated='TRUE', lastupdate=datetime() WHERE filesid=?"

        # self.execQueriesProgress(insertQuery, insertQueryValuesList, "Insert Files: ")
        # self.execQueriesProgress(updateQuery, updateQueryValuesList, "Update Files: ")

        self.execQueryMany(insertQuery, insertQueryValuesList, toprint, "Insert Files: ")
        self.execQueryMany(updateQuery, updateQueryValuesList, toprint, "Update Files: ")


    def insertHash(self, hash):
        """Inserts the hash into the database"""
        record = self.searchHash(hash).fetchone()
        if record == None:
            self.execQuery("INSERT INTO hashes (hash, lastupdate) VALUES(?, datetime())", (hash,))
            hashesid =self.c.lastrowid
            self.cacheHashesClass.add(hash, hashesid)
            return hashesid

        return record['hashesid']

    def fillCacheAllHashes(self, paths):
        for hash in self.getHashes(paths).fetchall():
            self.cacheHashesClass.add(hash['hash'], hash['hashesid'])



    def insertHashes(self, hashesQueue, paths=None, toprint=False):
        """Inserts the hashes into the database"""

        updateHashToFileIdValueList = list()
        updateHashToFilePathValueList = list()
        while not hashesQueue.empty():
            hash = hashesQueue.get()

            if hash.hash not in self.cacheHashesClass.hashes:
                self.insertHash(hash.hash)

            if hash.path in self.cacheFilesClass.files:
                updateHashToFileIdValueList.append((self.cacheHashesClass.getHashesid(hash.hash), self.cacheFilesClass.getFilesid(hash.path)))
            else:
                updateHashToFilePathValueList.append((self.cacheHashesClass.getHashesid(hash.hash), hash.path))

            # else:
            #     # Insert
            #     hashesid = self.insertHash(hash.hash)
            #
            #     if hash.path in self.cacheFiles.files:
            #         updateHashToFileIdValueList.append((hashesid, self.cacheFilesInPathsToDict[hash.path]['filesid']))
            #     else:
            #         updateHashToFilePathValueList.append((self.allHashesDict[hash.hash]['hashesid'], hash.path))

        # self.commit()

        updatePathQuery="UPDATE files SET hashesid = ? WHERE path = ? "
        updateIdQuery = "UPDATE files SET hashesid = ? WHERE filesid = ? "
        # self.execQueriesProgress(updateQuery, updateHashToFileValueList, "Insert Hashes record in DB. Number: ")

        self.execQueryMany(updateIdQuery, updateHashToFileIdValueList, toprint, "Insert Hashes record in DB. Number: ")
        self.execQueryMany(updatePathQuery, updateHashToFilePathValueList, toprint, "Insert Hashes record in DB. Number: ")


    def addHashToFile(self, hashesid, path):
        self.execQuery("UPDATE files SET hashesid = ? WHERE path = ? ", (hashesid, path))
        return self.c.lastrowid

    # def addHashToFileMany(self, hashesid, path):
    #     # if not hasattr(self,'updateHashToFileValueList'):  self.updateHashToFileValueList= list()
    #     self.updateHashToFileValueList.append((hashesid, path))
    #     # self.execQuery("UPDATE files SET hashesid = ? WHERE path = ? ", (hashesid, path))

    def searchFile(self, path):
        values = (path,)
        command = "SELECT f.filesid as filesid, h.hashesid as hashesid, f.path as path, f.bytes as bytes, h.hash as hash  " \
                  "FROM files f LEFT JOIN  hashes h on  h.hashesid  = f.hashesid  WHERE f.path=?"
        return self.execQuery(command, values)

    def searchHash(self, hash):
        return self.execQuery("SELECT * FROM hashes WHERE hash = ? ", (hash,))

    def getHashes(self, paths=None):
        if paths == None:
            return self.execQuery("SELECT * FROM hashes")
        else:
            query = "SELECT * FROM hashes LEFT JOIN files ON hashes.hashesid = files.hashesid WHERE "+" ".join(self.getFilePathsQueryList(paths))
            return self.execQuery(query)

    def findFilesWithSameSize(self, paths, Hashes, allhash=False):
        command2 = self.getFilePathsQueryList(paths)

        """ Get records with same SIZE """
        if not allhash:
            command1 = ["SELECT  bytes, count(*) as c FROM files WHERE"]
            command3 = ["GROUP BY bytes HAVING c > 1  ORDER BY c DESC"]
            command = command1 + command2 + command3  # GET all FILES with SAME SIZE
            if self.debug: print(" ".join(command))
            allsamesizeresults = self.execQuery(" ".join(command)).fetchall()

            # Calculate the HASH for FILES with same SIZE
            sizeList=list()
            for r in allsamesizeresults:
                sizeList.append(r['bytes'])

            sameSizeQuery = " ".join(["SELECT path FROM files WHERE"] + command2 +
                                     ["AND", "bytes IN ( "+str(sizeList).replace('[','').replace(']','')+" )"]+
                                     ["AND", "hashesid IS NULL"])
            samesize = self.execQuery(sameSizeQuery).fetchall()
            for r in samesize:
                self.addHash(Hashes, r['path'])

    def findAndInsertDuplicates(self,paths):
        duplicatesClass = Duplicates()
        duplicates = self.findDuplicates(paths)

        insertsList,updateList = duplicatesClass.insertOrUpdate(self.getDuplicates(paths), duplicates)
        self.insertDuplicates(insertsList, False)
        self.updateDuplicatesIsUpdated(updateList)

    def findDuplicates(self, paths):

        samehashesidQuery = "SELECT hashesid FROM files WHERE hashesid not like '' GROUP BY hashesid HAVING count(*) > 1"
        command1 = [
            "SELECT files.filesid, hashes.hashesid, files.path, files.bytes, hashes.hash",
            "FROM files  LEFT JOIN  hashes  on hashes.hashesid  = files.hashesid  WHERE "]
        command2 = self.getFilePathsQueryList(paths)
        command3 = ["AND", "hashes.hashesid IN (",samehashesidQuery , ")"]
        command4 = ["ORDER BY hashes.hashesid DESC"]
        command = command1 + command2 + command3 + command4

        return self.execQuery(" ".join(command)).fetchall()


    # def findDuplicates(self, paths):
    #     command2 = self.getFilePathsQueryList(paths)
    #
    #     print("Search for duplicates")
    #
    #     """ Get records with same HASH"""
    #     query = "SELECT hashesid, count(*) AS c FROM files WHERE hashesid not like '' GROUP BY hashesid HAVING c > 1 ORDER BY c DESC"
    #     allsamehashresults = self.execQuery(query).fetchall()
    #
    #
    #     print("Find Duplicates. Number of hashes: %i" % (len(allsamehashresults)))
    #     pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(allsamehashresults)).start()
    #     pbarCount = 0
    #
    #     # ADD duplicate for FILES with same HASH
    #     for r in allsamehashresults:
    #         if self.debug: print("Search for all the files with the hash: " + str(r['hash']))
    #         values = (r['hashesid'],)
    #         command1 = [
    #             "SELECT files.filesid as filesid, hashes.hashesid as hashesid, files.path as path, files.bytes as bytes, hashes.hash as hash",
    #             "FROM files  LEFT JOIN  hashes  on hashes.hashesid  = files.hashesid  WHERE "]
    #         command3 = ["AND", "hashes.hashesid = ?"]
    #         command = command1 + command2 + command3
    #         if self.debug: print(" ".join(command))
    #         samehashresults = self.execQuery(" ".join(command), values).fetchall()
    #
    #         for r_i in samehashresults:
    #             self.insertDuplicate(r_i['hashesid'], r_i['filesid'])
    #
    #         pbarCount += 1
    #         pbar.update(pbarCount)
    #
    #     pbar.finish()
    #     print("\n")
    #
    #     self.commit()


    def insertDuplicates(self, duplicatesList, update=True):
        if len(duplicatesList) == 0:
            return

        print("\n\t Insert Duplicate files. Number of files: %i" % (len(duplicatesList)))
        pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(duplicatesList)).start()
        pbarCount = 0

        for duplicate in duplicatesList:
            self.insertDuplicate(duplicate['hashesid'], duplicate['filesid'], update)
            pbarCount += 1
            pbar.update(pbarCount)

        pbar.finish()


    def insertDuplicate(self, hashesid, filesid, update=True):
        """ Add Duplicate Record """

        record = None
        values = (filesid, hashesid)
        if update: record = self.getDuplicateByIDs(hashesid, filesid).fetchone()

        if record == None:  # No previous record
            command = "INSERT INTO duplicates (filesid, hashesid, isUpdated,  lastupdate) VALUES(?, ?, 'TRUE', datetime())"
            self.execQuery(command, values)
        else:
            if update: self.execQuery("UPDATE duplicates SET isUpdated = 'TRUE' WHERE filesid = ? AND hashesid = ? ", values)


    def updateDuplicatesIsUpdated(self, updateList):
        if len(updateList)> 0 :
            return self.execQuery("UPDATE duplicates SET isUpdated = 'TRUE' WHERE duplicatesid IN ( ? ) ", (",".join(updateList),))

    def getDuplicateByIDs(self, hashesid, filesid):
        values = (hashesid, filesid)
        command = "SELECT * FROM duplicates WHERE hashesid = ? AND filesid = ?"
        results = self.execQuery(command, values)
        return results


    def getDuplicates(self, paths):
        return self.getDuplicatesWithFilesAndHashes(paths, None, None)

    def getDuplicatesWithFilesAndHashes(self, paths, isUpdated=None, groupBy=None):
        command1 = [ "SELECT * FROM duplicates",
                     "LEFT JOIN hashes ON duplicates.hashesid = hashes.hashesid",
                     "LEFT JOIN files ON duplicates.filesid = files.filesid", "WHERE"]
        command2 = self.getFilePathsQueryList(paths)
        command3 = []

        if isUpdated is not None:
            isUpdatedQueryPart = "duplicates.isUpdated = '%s'" % str(isUpdated).upper()
            command1 = command1 + [isUpdatedQueryPart, "AND"]

        if groupBy is not None:
            group = "GROUP BY "+str(groupBy)
            command3 = [group]
        command = command1 + command2 + command3
        # print(" ".join(command))
        return self.execQuery(" ".join(command))

    def getDuplicate(self, hashesid, paths):
        command2 = self.getFilePathsQueryList(paths)

        command1 = ["SELECT * FROM files LEFT JOIN duplicates on files.filesid = duplicates.filesid  WHERE"]
        command3 = ["AND", "files.filesid IN ( SELECT filesid FROM duplicates WHERE hashesid = ?) "]
        command = command1 + command2 + command3
        if self.debug: print(" ".join(command))
        return self.execQuery(" ".join(command), (hashesid,))

    def addHash(self, path):
        self.hashesClass.addFile(path)

    def getFilePathsQueryList(self, paths):
        if isinstance(paths, str): paths = [paths]

        pathsQuery = ["("]
        for path in paths:
            path = self.isPathWildcard(path)
            pathsQuery.append("files.path LIKE '" + path + "'")
            pathsQuery.append("OR")
        del pathsQuery[-1]
        pathsQuery.append(")")

        return pathsQuery #" ".join(pathsQuery)

    # def calcHashes(self, hashesClass, paths=None, maxsizehash=0):
    #     hashesClass.calcHashes(maxsizehash)
    #     self.insert_hashes(paths)

    def rmOldFiles(self, paths):
        """Delete all the files in the path with isUpdated TRUE"""
        if isinstance(paths, str): paths = [paths]

        command1 = ["SELECT files.filesid", "FROM files LEFT JOIN  hashes on  hashes.hashesid  = files.hashesid", "WHERE"]
        command2 = self.getFilePathsQueryList(paths)
        command3 = ["AND", "files.isUpdated='FALSE'"]
        command = command1 + command2 + command3
        results = self.execQuery(" ".join(command)).fetchall()

        for r in results:
            values = (r['filesid'],)
            self.execQuery("DELETE FROM duplicates WHERE filesid = ?", values)  # DELETE DUPLICATE record
            self.execQuery("DELETE FROM files WHERE filesid = ?", values)  # DELETE FILE record

        self.execQuery(
            "DELETE FROM duplicates WHERE isUpdated = 'FALSE'")  # DELETE DUPLICATE record with ISUPDATE = FALSE

    def deleteFilePath(self, filepath):
        querySelectFile="SELECT filesid FROM files WHERE path LIKE ?"
        fileid = self.execQuery(querySelectFile, (filepath,))
        for filesid in fileid.fetchall():
            fileid = filesid['filesid']

            values = (fileid,)
            self.execQuery("DELETE FROM duplicates WHERE filesid = ?", values)
            self.execQuery("DELETE FROM files WHERE filesid = ?", values)


    def cleanUnusedHashes(self):
        # TODO
        pass
        # if r['hashesid'] != None:
        #     self.execQuery("DELETE FROM hashes WHERE filesid = ?", values)    #DELETE HASH record

    def commit(self):
        self.conn.commit()

    def execQuery(self, query, values=None):
        if self.debug: print(" ".join(query) + " " + str(values))
        if values == None:
            return self.c.execute(query)
        else:
            return self.c.execute(query, values)

    def execQueriesProgress(self, query, valuesList, dispMsg="Number of queries: "):
        print("\n", dispMsg, len(valuesList))
        pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(valuesList)).start()
        pbarCount = 0
        for value in valuesList:
            self.execQuery(query, value)
            pbarCount += 1
            pbar.update(pbarCount)

        pbar.finish()
        self.commit()

    def execQueryMany(self, query, valuesList, toprint=False, dispMsg="Number of queries: "):
        if len(valuesList) > 0:
            if self.debug: print(query + " " + str(valuesList))
            # print("\n"+query + " " + str(len(valuesList)))
            chuncks=list(self.execQueryManyChunks(valuesList, 50))

            if toprint: print("\n", dispMsg, len(chuncks))
            if toprint: pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(chuncks)).start()
            if toprint: pbarCount =0

            for partialValuesList in chuncks:
                # print(partialValuesList)
                self.c.executemany(query, partialValuesList)
                if toprint: pbarCount += 1
                if toprint: pbar.update(pbarCount)

                if toprint: pbar.finish()
            # self.commit()

    def execQueryManyChunks(self, valuesList, chunckSize):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(valuesList), chunckSize):
            yield valuesList[i:i + chunckSize]

    def close(self):
        self.commit()
        self.conn.close()






class CacheFiles():

    def __init__(self):
        self.files = dict()

    def add(self, path, filesid, bytes, hashesid=None):
        self.files[path] = {'filesid': filesid, 'path': path, 'size': bytes, 'hashesid': hashesid}

    def search(self, path):
        return self.files[path]

    def getFilesid(self, path):
        return self.search(path)['filesid']

    def getHashesid(self, path):
        return self.files[path]['hashesid']

    def sameSize(self, path, size):
        if self.files[path]['size'] == size:
            return True
        else:
            return False

    def __len__(self):
        return len(self.files)


class CacheHashes():

    def __init__(self):
        self.hashes = dict()

    def add(self, hash, hashid):
        self.hashes[hash] = {'hashid': hashid, 'hash': hash}

    def search(self, hash):
        return self.hashes[hash]

    def getHashesid(self, hash):
        return self.search(hash)['hashid']

    def __len__(self):
        return len(self.hashes)


class Files(object):
    """Takes charge of harvesting file data"""

    def __init__(self, path, threads=1, size=0):
        self.path = path
        self.size = size
        self.ThreadNum = threads
        self.debug = False
        self.filesList = list()
        self.filesDetailsQueue = Queue(0)
        self.pbar = None
        self.pbarCount = 0


    def findFiles(self):
        """Walks through and entire directory to find all the files."""
        for root, dir, files in os.walk(self.path):
            for file in files:
                absolute = os.path.join(root, file)
                if os.path.isfile(absolute) and os.path.islink(absolute) == False:
                    self.filesList.append(absolute)

        if len(self.filesList) == 0:
            return self.filesDetailsQueue

        print("\n\t Number of files to scan : %i" % len(self.filesList))
        self.pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(self.filesList)).start()
        self.pbarCount = 0

        p = ThreadPool(self.ThreadNum)
        p.map(self.getFileDetails, self.filesList)
        p.close()

        self.pbar.finish()

        return self.filesDetailsQueue


    def getFileDetails(self, path):
        try:
            path = str(path).strip()
            if path:    # path not empty
                size = os.path.getsize(path)
                if size > self.size:
                    self.filesDetailsQueue.put(FileDetail(path,size))
                    if self.debug: print("File %s: %s" % (str(path), str(size)))
        except:
            print("Error to open the file", path)

        self.pbarCount += 1
        self.pbar.update(self.pbarCount)





class FileDetail():
    def __init__(self, path, size):
        self.path = str(path).strip()
        self.size = int(size)

    def __str__(self):
        return str(str(self.path)+" "+str(self.size))



class Hashes():

    def __init__(self, threadsNum=3, debug=False):
        self.debug = debug
        self.filesList = list()
        self.hashesQueue = Queue(0)
        self.ThreadNum = threadsNum
        self.pbarCount = 0
        self.maxsizehash = None

    def addFile(self, path):
        self.filesList.append(path)

    def sizeFiles(self):
        return len(self.filesList)

    def calcHashes(self):
        if len(self.filesList) == 0:
            return self.hashesQueue

        print("\n\t Calculate Hash. Number of files: %i" % (len(self.filesList)))
        self.pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(self.filesList)).start()

        p = ThreadPool(self.ThreadNum)
        p.map(self._calcHash, self.filesList)
        p.close()

        self.pbar.finish()
        return self.hashesQueue


    def _calcHash(self, path):
        # To give time to the DB to process the insert hashes queries and don't accumulate them into memory
        while self.hashesQueue.qsize() > 1000:
            time.sleep(2)

        if self.debug: print("Calculate SHA hash of the file: " + str(path))
        if self.maxsizehash == 0:  # hash all file
            readfile = open(path, 'rb', buffering=0).read()
        else:
            readfile = open(path, 'rb', buffering=0).read(self.maxsizehash)

        fileHashes = HashedFile(path, hashlib.sha512(readfile).hexdigest())
        self.hashesQueue.put(fileHashes)

        self.pbarCount += 1
        self.pbar.update(self.pbarCount)



class HashedFile:
    def __init__(self, path, hash):
        self.path = path
        self.hash = hash

    def __str__(self):
        return str(str(self.path+" "+str(self.hash)))


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
            self.full_report()

    # def setDebug(self, debug=False):
    #     self.debug=debug

    def full_report(self, paths, export=None):
        """Returns all reports available"""
        self.report_count_duplicates(paths)
        self.report_list_duplicates(paths, export)
        self.report_analysis_duplicates()


    def report_count_duplicates(self, paths):
        self.duplicatedFilesCount = len(self.DB.getDuplicatesWithFilesAndHashes(paths).fetchall())
        print("")
        print("--------------------------------------")
        print("Number of duplicated files are: %i" % (self.duplicatedFilesCount))

    def report_list_duplicates(self, paths, export=None):
        duplicatedFilesList = dict()
        duplicatedFilesSize = 0
        if export is not None:  f = open(export, 'w')
        print("")
        print("--------------------------------------")
        print("Duplicate files: ")
        for r in self.DB.getDuplicatesWithFilesAndHashes(paths, "TRUE", "hashes.hashesid").fetchall():
            hash = self.DB.getDuplicate(r['hashesid'], paths).fetchall()
            if len(hash) > 1:
                files = []
                c = 0
                for d in hash:
                    c += 1
                    files.append('"'+str(d['path'])+'"')
                    if c > 0: duplicatedFilesSize += r['bytes']


                line = "%s\t%s\t%s" % (r['hash'], self.human_value(r['bytes']), " ".join(files))
                duplicatedFilesList[r['hash']] = { "hash":r['hash'], "files":files, "size":r['bytes'] }

                print(line)
                if export is not None:  f.write(line+"\n")

        if export is not None:  f.close()
        self.duplicatedFilesList = duplicatedFilesList
        self.duplicatedFilesSize = duplicatedFilesSize


    def report_analysis_duplicates(self):
        print("--------------------------------------")
        print("Liten3 Full Reporting")
        print("Duplicate files found: %i" % (self.duplicatedFilesCount))
        print("Total space wasted: %s " % (self.human_value(self.duplicatedFilesSize)))

        print("")
        print("To delete files, run liten3 in interactive mode: python liten3.py -i")



    def human_value(self, value):
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


class Duplicates():

    def __init__(self):
        self.duplicateList = list()

    def insertOrUpdate(self, dbRecords, newDuplicate):
        # hashesid = dict()
        duplicateRecords = dict()
        duplicatesid = dict()

        for rec in dbRecords:
            if rec['hash'] not in duplicateRecords:
                duplicateRecords[rec['hash']] = list()
                # hashesid[rec['hashes.hash']] = rec['hashes.hashesid']
            duplicateRecords[rec['hash']].append(rec['filesid'])
            duplicatesid[rec['filesid']] = rec['duplicatesid']

        inserts = list()
        updates = list()
        for rec in newDuplicate:
            if rec['hash'] not in duplicateRecords or rec['filesid'] not in duplicateRecords[rec['hash']]:
                inserts.append({'hashesid':rec['hashesid'], 'filesid':rec['filesid']})
            else:
                updates.append(str(duplicatesid[rec['filesid']]))

        return inserts,updates


class Interactive(object):
    """This mode creates a session to delete files"""

    def __init__(self,DB, dryrun=False):
        self.DB = DB
        self.dryrun = dryrun
        self.debug = False
        self.autoDelete = False


    # def setDebug(self, debug=False):
    #     self.debug=debug

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

        forDeletion = list()

        try:
            for r in self.DB.getDuplicatesWithFilesAndHashes(paths, groupBy="duplicates.hashesid").fetchall():
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
                            forDeletion = forDeletion + files

                if not self.autoDelete:
                    try:
                        answer = True
                        while answer:
                            choose = int(input("Choose a number to delete (Enter to skip): "))
                            if filepaths[choose] not in forDeletion:
                                forDeletion.append(filepaths[choose])
                            if not choose:
                                answer = False

                    except ValueError:
                        print("--------------------------------------------------\n")

            print("Files selected for complete removal:\n")
            for selection in forDeletion:
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
                    for selection in forDeletion:
                        if selection:
                            try:
                                print("Removing file:\t %s" % selection)
                                os.remove(selection)
                                self.DB.deleteFilePath(selection)
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
        toDelete = list()

        for file in filesPath:
            if file != "": #not empty content
                for folder in self.deletePaths:
                    if file.startswith(folder):
                        toDelete.append(file)

        #If there is a FALSE at least one copy remains in the system
        if len(toDelete) < len(filesPath):
            return toDelete
        else:
            return None

################# LINE40 #############################









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


def timestr():
    return str(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))

def checkOptions(options):

    """ Test options"""
    if options.open and options.tmpDB:
        sys.stderr.write("Do you want to use a temporary database (--tmpDB) or not?")
        sys.exit(1)


    """ Setting  """
    #SIZE
    if options.size:
        options.size = int(options.size) * 1048576
    else:
        options.size = 0

    #MaxSizeHash
    if options.maxsizehash:
        options.maxsizehash = int(options.maxsizehash) * 1048576
    else:
        options.maxsizehash = 0

    #PATHS
    if options.path and len(options.path) > 0:
        absolutePath = list()
        for i in options.path:
            absolutePath.append(os.path.abspath(i))
        options.path = absolutePath
        options.searchduplicate = True
        options.report = options.path

    if options.delete and len(options.delete) > 0:
        absolutePath = list()
        for i in options.delete:
            absolutePath.append(os.path.abspath(i))
        options.delete = absolutePath

    # THREADs
    if options.threads:
        options.threads = int(options.threads)
    else:
        options.threads = int(cpu_count())


    return options


def main():
    """Parse the options"""
    p = OptionParser(option_class=MultipleOption,  usage='usage: %prog [OPTIONS]')
    p.add_option('--open', '-o', help="Use a specific database")
    p.add_option('--tmpDB', '-t', action="store_true", help="Use a temporary in-memory database")
    p.add_option('--path', '-p', action="extend", type="string", help="Supply a path to search")
    p.add_option('--delete', '-x', action="extend", type="string", help="Delete all the duplicates files in the provided path")
    p.add_option('--size', '-s', help="Search by Size in MB. By defaults to 0")
    p.add_option('--export', '-e', help="Export the list of duplicate file into text file")
    p.add_option('--maxsizehash', '-m', help="Set the maximum size in MB to hash.")
    p.add_option('--threads', '-u', help="Number of threads to use. By defaults use the same number of CPU cores")
    p.add_option('--hashall','-a', action="store_true",help="Hash all the files. By defauls it hash only files with the same size.")
    p.add_option('--dryrun', '-d', action="store_true", help="Does not delete anything. Use ONLY with interactive mode")
    p.add_option('--report', '-r', action="store_true", help="Generates a report from a previous run")
    p.add_option('--interactive', '-i', action="store_true", help='Interactive mode to delete files')
    p.add_option('--searchduplicate', '-D', action="store_true", help='Search duplicate files only')
    options, arguments = p.parse_args()
    options = checkOptions(options)

    #DB
    dbfilename = str(pathlib.Path.home()) + "/.liten3.sqlite"
    if options.open:  dbfilename = os.path.abspath(options.open)
    if options.tmpDB: dbfilename = ':memory:'

    dbClass = Db(dbfilename, options.threads)
    dbClass.debug = debug

    hashesClass = Hashes(options.threads)
    hashesClass.maxsizehash = options.maxsizehash
    dbClass.hashesClass = hashesClass

    cacheFilesClass = CacheFiles();  cacheHashesClass = CacheHashes()
    dbClass.cacheFilesClass = cacheFilesClass;  dbClass.cacheHashesClass = cacheHashesClass



    """ Start program """
    if options.path:
        for path in options.path:
            if not os.path.isdir(path):
                sys.stderr.write("Search path does not exist or is not a directory: %s\n" % path)
                sys.exit(1)

        for path in options.path:
            try:
                filesClass = Files(path, options.threads, options.size)
                filesClass.debug = debug

                print("%s\t Load in memory file details for path %s" %(timestr(), path))
                dbClass.fillCacheFilesInPaths(path)
                dbClass.setIsUpdated(path)

                print("%s\t Search files in folder: %s." % (timestr(), path))
                filesDetailsProcess = multiprocessing.Process(target=filesClass.findFiles)
                filesDetailsProcess.start()

                while filesDetailsProcess.is_alive():
                    dbClass.insertFiles(path, filesClass.filesDetailsQueue, options.hashall)

                print("%s\t Insert in DB files in folder: %s." % (timestr(), path))
                dbClass.insertFiles(path, filesClass.filesDetailsQueue, options.hashall)

                filesDetailsProcess.terminate()
                dbClass.commit()


            except (KeyboardInterrupt, SystemExit):
                print("Exiting nicely from Liten3...")
                sys.exit(1)


        print("%s\t Clean DB files from old files." % (timestr()))
        dbClass.rmOldFiles(options.path)

        print("%s\t Looking for Files with same size" % (timestr()))
        dbClass.findFilesWithSameSize(options.path, hashesClass, options.hashall)

        print("%s\t Load in memory hashes for path %s" % (timestr(), str(options.path)))
        dbClass.fillCacheFilesInPaths(options.path)
        dbClass.fillCacheAllHashes(options.path)

        if hashesClass.sizeFiles() > 0:
            print("%s\t Calculate hashes for files in path %s" % (timestr(), str(options.path)))
            calcHashesProcess = multiprocessing.Process(target=hashesClass.calcHashes)
            calcHashesProcess.start()

            while calcHashesProcess.is_alive():
                dbClass.insertHashes(hashesClass.hashesQueue, options.path, False)

            print("%s\t Insert in DB hashes in folder: %s." % (timestr(), str(options.path)))
            dbClass.insertHashes(hashesClass.hashesQueue, options.path, False)

            calcHashesProcess.terminate()

        dbClass.commit()


    if options.searchduplicate:
        if not options.path: options.path = '/'
        print("%s\t Looking for Duplicated files with same Hash." %(timestr()))
        dbClass.findAndInsertDuplicates(options.path)


    if options.report:
        reportClass = Report(dbClass)
        if not options.path: options.path = "/"
        if options.export:
            reportClass.full_report(options.path, options.export)
        else:
            reportClass.full_report(options.path, None)


    if options.interactive or options.delete:
        interactiveClass = Interactive(dbClass)
        interactiveClass.debug = debug
        if options.dryrun:
            interactiveClass.dryrun=True


        if options.delete: interactiveClass.delete(options.delete)
        if not options.path:
            interactiveClass.session("%")
        else:
            interactiveClass.session(options.path)



    """ EXIT """
    dbClass.close()
    sys.exit(0)


if __name__=="__main__":
    main()
