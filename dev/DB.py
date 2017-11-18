import os
import sqlite3
import time
import hashlib

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

            #Calculate the HASH for FILES with same SIZE
            for r in allsamesizeresults:
                samesize = self.c.execute("SELECT path FROM files WHERE bytes = ?", (r['bytes'],)).fetchall()
                for r_i in samesize:
                    self.addHash(r_i['path'])


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
