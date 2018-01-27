import os, time, logging
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

        logging.info("Number of files to scan : %i" % len(self.filesList))
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
                    logging.debug("File %s: %s" % (str(path), str(size)))
        except:
            logging.error("Error to open the file", path)

        self.pbarCount += 1
        self.pbar.update(self.pbarCount)





class FileDetail():
    def __init__(self, path, size):
        self.path = str(path).strip()
        self.size = int(size)

    def __str__(self):
        return str(str(self.path)+" "+str(self.size))
