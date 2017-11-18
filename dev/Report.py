

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

