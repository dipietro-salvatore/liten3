import os
import sqlite3

from progressbar import Bar, Percentage, ProgressBar
from Duplicates import Duplicates


class DbWork(object):
    """ All the Database work happens here """

    def __init__(self, dbfilename, threads, debug=False):
        self.debug = debug
        self.database = dbfilename
        self.connection()
        self.threads = threads

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

    # def are_paths_wildcard(self, paths):
    #     """ Check if a list of paths have the wildcard in the end of it (/%). If not it will add it."""
    #     for i in range(len(paths)):
    #         paths[i] = self.is_path_wildcard(paths[i])
    #
    #     return paths

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


    def getFilesInPath(self, path):
        path = self.isPathWildcard(path)

        q = "SELECT filesid,path,bytes,hashesid FROM files WHERE path LIKE ?"
        return self.execQuery(q,(path,))

    def insertFiles(self, pathFolder, filesDetailQueue, getHash=False):
        """Inserts the files information into the database"""

        allFiles = self.getFilesInPath(pathFolder).fetchall()
        filesDict = dict()
        for file in allFiles:
            filesDict[file['path']] = {'filesid':file['filesid'], 'path':file['path'], 'size':file['bytes'], 'hashesid':file['hashesid']}

        insertQuery = "INSERT INTO files (path, bytes, hashesid, isUpdated, lastupdate) VALUES (?, ?, NULL, 'TRUE', datetime())"
        updateQuery = "UPDATE files SET bytes=?, isUpdated='TRUE', lastupdate=datetime() WHERE filesid=?"
        insertQueryValuesList = list()
        updateQueryValuesList = list()
        ci=0
        cu=0

        while not filesDetailQueue.empty():
            file = filesDetailQueue.get()
            if file.path in filesDict:  #Update
                cu+=1
                value = (file.size, filesDict[file.path]['filesid'],)
                updateQueryValuesList.append( value )
                self.execQuery(updateQuery, value)
                if filesDict[file.path]['size'] != file.size and filesDict[file.path]['hashesid'] != None: self.addHash(file.path)
            else:  # Insert
                ci+=1
                value = (file.path, file.size,)
                insertQueryValuesList.append( value )
                self.execQuery(insertQuery, value)
                if getHash: self.addHash(file.path)

        # print("INSERT: "+str(len(insertQueryValuesList))+" "+str(ci))
        # print("UPDATE: " + str(len(updateQueryValuesList))+" "+str(cu))
        self.commit()

    def insert_hash(self, hash):
        """Inserts the hash into the database"""
        record = self.searchHash(hash).fetchone()
        if record == None:
            self.execQuery("INSERT INTO hashes (hash, lastupdate) VALUES(?, datetime())", (hash,))
            return self.c.lastrowid

        return record['hashesid']


    def insertHashes(self, hashesQueue, paths=None):
        """Inserts the hashes into the database"""

        allHashesDict=dict()
        allHashes=self.getHashes(paths)
        for hash in allHashes.fetchall():
            allHashesDict[hash['hash']]={'hashesid':hash['hashesid'], 'hash':hash['hash']}

        while not hashesQueue.empty():
            hash = hashesQueue.get()
            if hash.hash in allHashesDict:  # Update
                self.addHashToFile(allHashesDict[hash.hash]['hashesid'], hash.path)
            else:  # Insert
                hashesid = self.insert_hash(hash.hash)
                allHashesDict[hash.hash]={'hashesid':hashesid, 'hash':hash.hash}
                self.addHashToFile(hashesid, hash.path)

        self.commit()

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

            sameSizeQuery = " ".join(["SELECT path FROM files WHERE"] + command2 + ["AND bytes IN ( "+str(sizeList).replace('[','').replace(']','')+" )"])
            samesize = self.execQuery(sameSizeQuery).fetchall()
            for r in samesize:
                self.addHash(Hashes, r['path'])

    def findAndInsertDuplicates(self,paths):
        print("\nLooking for duplicated files")
        duplicatesClass = Duplicates()
        duplicates = self.findDuplicates(paths)
        # self.insertDuplicates(duplicates)
        print(len(duplicates))
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

        print("Insert Duplicate files. Number of files: %i" % (len(duplicatesList)))
        pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(duplicatesList)).start()
        pbarCount = 0

        for duplicate in duplicatesList:
            self.insertDuplicate(duplicate['hashesid'], duplicate['filesid'], update)
            pbarCount += 1
            pbar.update(pbarCount)

        pbar.finish()
        print("\n")

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

    def addHash(self, hashesClass, path):
        hashesClass.addFile(path)

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

    def close(self):
        self.commit()
        self.conn.close()