import os
from progressbar import Bar, Percentage, ProgressBar


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

        #self.db.rmOldFiles(self.path)
        #self.db.findDuplicates(self.path)
        # self.db.showduplicates(self.path)
