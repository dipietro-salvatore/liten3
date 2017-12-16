import os, time
import sqlite3

from progressbar import Bar, Percentage, ProgressBar
from Duplicates import Duplicates


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

        updatePathQuery="UPDATE files SET hashesid = ? WHERE path = ? "
        updateIdQuery = "UPDATE files SET hashesid = ? WHERE filesid = ? "

        self.execQueryMany(updateIdQuery, updateHashToFileIdValueList, toprint, "Insert Hashes record in DB. Number: ")
        self.execQueryMany(updatePathQuery, updateHashToFilePathValueList, toprint, "Insert Hashes record in DB. Number: ")


    def addHashToFile(self, hashesid, path):
        self.execQuery("UPDATE files SET hashesid = ? WHERE path = ? ", (hashesid, path))
        return self.c.lastrowid

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

        return pathsQuery


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