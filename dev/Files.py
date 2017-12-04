import os
from multiprocessing.pool import ThreadPool
from multiprocessing import Queue
from progressbar import Bar, Percentage, ProgressBar


class Files(object):
    """Takes charge of harvesting file data"""

    def __init__(self, path, threads=1, size=0):
        self.path = path
        self.size = size
        self.ThreadNum = threads
        self.debug = False
        self.filesList = list()
        self.filesDetailsQueue = Queue()
        self.pbar = None
        self.pbarCount = 0


    def findFiles(self):
        """Walks through and entire directory to find all the files."""

        print("\n")
        print("Search files in folder: %s." % (self.path))

        for root, dir, files in os.walk(self.path):
            for file in files:
                absolute = os.path.join(root, file)
                if os.path.isfile(absolute) and os.path.islink(absolute) == False:
                    self.filesList.append(absolute)

        print("\n")
        print("Number of files to scan : %i" % len(self.filesList))
        self.pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(self.filesList)).start()
        self.pbarCount = 0


        p = ThreadPool(self.ThreadNum)
        p.map(self.getFileDetails, self.filesList)
        p.close()

        self.pbar.finish()
        print("\n")

        return self.filesDetailsQueue


    def getFileDetails(self, path):
        size = os.path.getsize(path)
        if size > self.size:
            self.filesDetailsQueue.put(FileDetail(path,size))
            if self.debug: print("File %s: %s" % (str(path), str(size)))

        self.pbarCount += 1
        self.pbar.update(self.pbarCount)


class FileDetail():
    def __init__(self, path, size):
        self.path = path
        self.size = size

    def __str__(self):
        return str(str(self.path)+" "+str(self.size))
