

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

