from multiprocessing.pool import ThreadPool
from multiprocessing import Queue
from progressbar import Bar, Percentage, ProgressBar
import hashlib



class Hashes():

    def __init__(self, threadsNum=3, debug=False):
        self.debug = debug
        self.filesList = list()
        self.hashesQueue = Queue()
        self.ThreadNum = threadsNum
        self.pbarCount = 0

    def addFile(self, path):
        self.filesList.append(path)

    def calcHashes(self, maxsizehash=0):
        self.maxsizehash = maxsizehash
        print("\n\n")
        print("Calculate Hash. Number of files: %i" % (len(self.filesList)))
        self.pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(self.filesList)).start()

        p = ThreadPool(self.ThreadNum)
        p.map(self.calcHash, self.filesList)
        p.close()

        self.pbar.finish()
        print("\n")

        return self.hashesQueue

    def calcHash(self, path):
        if self.debug: print("Calculate SHA hash of the file: " + str(path))
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