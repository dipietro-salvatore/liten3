from multiprocessing.pool import ThreadPool
from multiprocessing import Queue, current_process
import multiprocessing
from progressbar import Bar, Percentage, ProgressBar
import hashlib, time, logging



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

            logging.info("Calculate Hash. Number of files: %i" % (len(self.filesList)))
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

        logging.debug("Calculate SHA hash of the file: " + str(path))
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