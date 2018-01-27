import logging, time


class Report(object):

    def __init__(self, DB, full=False):
        """Builds a full or specific report"""
        self.full = full
        self.dbClass = DB
        self.debug = False

        self.duplicatedFilesCount = None
        self.duplicatedFilesList = None
        self.duplicatedFilesSize = None

        if self.full:
            self.fullReport()


    def fullReport(self, paths, export=None, searchDuplicate=True, searchUnique=False):
        """Returns all reports available"""
        if searchDuplicate:
            self.reportCountDuplicates(paths)
            self.reportListDuplicates(paths, export)
            time.sleep(1)

        if searchUnique:
            self.reportListUnique(paths, export)
            time.sleep(1)

        self.reportAnalysis(searchDuplicate, searchUnique)

    def reportListUnique(self, paths, export=None):
        if export is not None:  f = open(str(export)+"_unique", 'w')
        logging.info("")
        logging.info("--------------------------------------")
        logging.info("Unique files: ")
        uniqueFiles = self.dbClass.findUniqueFilesQuery(paths).fetchall()
        self.uniqueFilesCount = len(uniqueFiles)
        for file in uniqueFiles:
            line = "%s\t%s\t%s" % (file['hash'], self.humanValue(file['bytes']), str(file['path']))
            print(line)
            if export is not None:  f.write(line + "\n")

        if export is not None:  f.close()


    def reportCountDuplicates(self, paths):
        self.duplicatedFilesCount = len(self.dbClass.getDuplicatesWithFilesAndHashes(paths).fetchall())
        logging.info("")
        logging.info("--------------------------------------")
        logging.info("Number of duplicated files are: %i" % (self.duplicatedFilesCount))

    def reportListDuplicates(self, paths, export=None):
        duplicatedFilesList = dict()
        duplicatedFilesSize = 0
        if export is not None:  f = open(str(export)+"_duplicated", 'w')
        logging.info("")
        logging.info("--------------------------------------")
        logging.info("Duplicate files: ")
        for r in self.dbClass.getDuplicatesWithFilesAndHashes(paths, "TRUE", "hashes.hashesid").fetchall():
            hash = self.dbClass.getDuplicate(r['hashesid'], paths).fetchall()
            if len(hash) > 1:
                files = []
                c = 0
                for d in hash:
                    c += 1
                    files.append('"'+str(d['path'])+'"')
                    if c > 0: duplicatedFilesSize += r['bytes']


                line = "%s\t%s\t%s" % (r['hash'], self.humanValue(r['bytes']), " ".join(files))
                duplicatedFilesList[r['hash']] = { "hash":r['hash'], "files":files, "size":r['bytes'] }

                print(line)
                if export is not None:  f.write(line+"\n")

        if export is not None:  f.close()
        self.duplicatedFilesList = duplicatedFilesList
        self.duplicatedFilesSize = duplicatedFilesSize


    def reportAnalysis(self, searchDuplicate=True, searchUnique=True):
        print("\n\n--------------------------------------")
        print("Liten3 Full Reporting")
        if searchDuplicate:
            print("Duplicate files found: %i" % (self.duplicatedFilesCount))
            print("Total space wasted: %s " % (self.humanValue(self.duplicatedFilesSize)))

        if searchUnique:
            print("Unique files found: %i" % (self.uniqueFilesCount))

        print("")
        print("To delete files, run liten3 in interactive mode: python liten3.py -i")



    def humanValue(self, value):
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

