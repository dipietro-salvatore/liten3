#!/usr/bin/env python
#Liten3 - Deduplication command line tool
#Author: Salvatore Dipietro (Initial Development by Noah Gift and Alfredo Deza)
#License: GPLv3 License
#Email: dipietro.salvatore [at] gmail dot com

__version__ = "3.0.2"
__date__ = "2018-01-27"


"""
Liten3 walks through a given path and creates a Checksum based on
the Sha512 library storing all the data in a Sqlite3 database.
You can run different kinds of reports once this has been done,
which gives you the ability to determine what files to delete.
"""


import sys, multiprocessing, os,pathlib
import sqlite3, time, hashlib
import logging
from optparse import Option, OptionParser
from progressbar import Bar, Percentage, ProgressBar
from multiprocessing.pool import ThreadPool
from multiprocessing import Queue, cpu_count
import hashlib

from DB import Db, CacheFiles, CacheHashes
from Report import Report
from Files import Files
from Hashes import Hashes
from Interactive import Interactive

debug = False





################# LINE40 #############################


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)


# URL https://stackoverflow.com/questions/4109436/processing-multiple-values-for-one-single-option-using-getopt-optparse
class MultipleOption(Option):
    ACTIONS = Option.ACTIONS + ("extend",)
    STORE_ACTIONS = Option.STORE_ACTIONS + ("extend",)
    TYPED_ACTIONS = Option.TYPED_ACTIONS + ("extend",)
    ALWAYS_TYPED_ACTIONS = Option.ALWAYS_TYPED_ACTIONS + ("extend",)

    def take_action(self, action, dest, opt, value, values, parser):
        if action == "extend":
            values.ensure_value(dest, []).append(value)
        else:
            Option.take_action(self, action, dest, opt, value, values, parser)


def timestr():
    return str(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))

def checkOptions(options):

    """ Test options"""
    if options.open and options.tmpDB:
        logging.error("Do you want to use a temporary database (--tmpDB) or not?")
        sys.exit(1)


    """ Setting  """
    #SIZE
    if options.size:
        options.size = int(options.size) * 1048576
    else:
        options.size = 0

    # Hashall
    if not options.hashall:
        options.hashall = False

    #MaxSizeHash
    if options.maxsizehash:
        options.maxsizehash = int(options.maxsizehash) * 1048576
    else:
        options.maxsizehash = 0

    #PATHS
    duplicates = False
    if options.path and len(options.path) > 0:
        absolutePath = list()
        for i in options.path:
            absolutePath.append(os.path.abspath(i))
        options.path = absolutePath
        options.searchduplicate = True
        options.report = options.path

    if options.delete and len(options.delete) > 0:
        absolutePath = list()
        for i in options.delete:
            absolutePath.append(os.path.abspath(i))
        options.delete = absolutePath

    # THREADs
    if options.threads:
        options.threads = int(options.threads)
    else:
        options.threads = int(cpu_count())

    # nosearchduplicate
    if options.nosearchduplicate:
        options.searchduplicate=False

    return options


def main():
    """Parse the options"""
    p = OptionParser(option_class=MultipleOption,  usage='usage: %prog [OPTIONS]')
    p.add_option('--open', '-o', help="Use a specific database")
    p.add_option('--tmpDB', '-t', action="store_true", help="Use a temporary in-memory database")
    p.add_option('--path', '-p', action="extend", type="string", help="Supply a path to search")
    p.add_option('--delete', '-x', action="extend", type="string", help="Delete all the duplicates files in the provided path")
    p.add_option('--size', '-s', help="Search by Size in MB. By defaults to 0")
    p.add_option('--export', '-e', help="Export the list of duplicate file into text file")
    p.add_option('--maxsizehash', '-m', help="Set the maximum size in MB to hash.")
    p.add_option('--threads', '-u', help="Number of threads to use. By defaults use the same number of CPU cores")
    p.add_option('--hashall','-a', action="store_true",help="Hash all the files. By defauls it hash only files with the same size.")
    p.add_option('--dryrun', '-n', action="store_true", help="Does not delete anything. Use ONLY with interactive mode")
    p.add_option('--report', '-r', action="store_true", help="Generates a report from a previous run")
    p.add_option('--noreport', '-R', action="store_true", help="Does not generates a report.")
    p.add_option('--interactive', '-i', action="store_true", help='Interactive mode to delete files')
    p.add_option('--searchduplicate', '-d', action="store_true", help='Search duplicate files only (by default)')
    p.add_option('--nosearchduplicate', '-D', action="store_true", help='Don\'t search duplicate files.')
    p.add_option('--searchunique', '-U', action="store_true", help='Search unique files only (Use with -r -D ).')
    options, arguments = p.parse_args()
    options = checkOptions(options)

    #DB
    dbfilename = str(pathlib.Path.home()) + "/.liten3.sqlite"
    if options.open:  dbfilename = os.path.abspath(options.open)
    if options.tmpDB: dbfilename = ':memory:'

    dbClass = Db(dbfilename, options.threads)
    dbClass.debug = debug

    hashesClass = Hashes(options.threads)
    hashesClass.maxsizehash = options.maxsizehash
    dbClass.hashesClass = hashesClass

    cacheFilesClass = CacheFiles();  cacheHashesClass = CacheHashes()
    dbClass.cacheFilesClass = cacheFilesClass;  dbClass.cacheHashesClass = cacheHashesClass



    """ Start program """
    if options.path and not options.report:
        for path in options.path:
            if not os.path.isdir(path):
                logging.error("Search path does not exist or is not a directory: %s\n" % path)
                sys.exit(1)

        for path in options.path:
            try:
                filesClass = Files(path, options.threads, options.size)
                filesClass.debug = debug

                logging.info("Load in memory file details for path %s" %(path))
                dbClass.fillCacheFilesInPaths(path)
                dbClass.setIsUpdated(path)

                logging.info("Search files in folder: %s." % (path))
                filesDetailsProcess = multiprocessing.Process(target=filesClass.findFiles)
                filesDetailsProcess.start()

                while filesDetailsProcess.is_alive():
                    dbClass.insertFiles(path, filesClass.filesDetailsQueue, options.hashall)

                logging.info("Insert in DB files in folder: %s." % (path))
                dbClass.insertFiles(path, filesClass.filesDetailsQueue, options.hashall)

                filesDetailsProcess.terminate()
                dbClass.commit()


            except (KeyboardInterrupt, SystemExit):
                logging.error("Exiting nicely from Liten3...")
                sys.exit(1)

        logging.info("Clean DB files from old files.")
        dbClass.rmOldFiles(options.path)

        logging.info("Looking for Files with same size")
        dbClass.findFilesWithSameSize(options.path, options.hashall)

        logging.info("Load in memory hashes for path %s" % (str(options.path)))
        dbClass.fillCacheFilesInPaths(options.path)
        dbClass.fillCacheAllHashes()

        if hashesClass.sizeFiles() > 0:
            logging.info("Calculate hashes for files in path %s" % (str(options.path)))
            calcHashesProcess = multiprocessing.Process(target=hashesClass.calcHashes)
            calcHashesProcess.start()

            while calcHashesProcess.is_alive():
                dbClass.insertHashes(hashesClass.hashesQueue, options.path, False)

            logging.info("Insert in DB hashes in folder: %s." % (str(options.path)))
            dbClass.insertHashes(hashesClass.hashesQueue, options.path, False)

            calcHashesProcess.terminate()

        dbClass.commit()


    if options.searchduplicate and not options.nosearchduplicate:
        if not options.path: options.path = '/'
        logging.info("Looking for Duplicated files with same Hash.")
        dbClass.findAndInsertDuplicates(options.path)

    if options.searchunique:
        if not options.path: options.path = '/'
        logging.info("Looking for Unique files.")
        dbClass.findUniqueFilesQuery(options.path)


    if options.report and not options.noreport:
        reportClass = Report(dbClass)
        if not options.path: options.path = "/"
        if not options.export: options.export = None
        reportClass.fullReport(options.path, options.export, options.searchduplicate, options.searchunique)


    if (options.interactive or options.delete) and (not options.searchunique) :
        interactiveClass = Interactive(dbClass)
        interactiveClass.debug = debug
        if options.dryrun:
            interactiveClass.dryrun=True


        if options.delete: interactiveClass.delete(options.delete)
        if not options.path:
            interactiveClass.session("%")
        else:
            interactiveClass.session(options.path)



    """ EXIT """
    dbClass.close()
    sys.exit(0)


if __name__=="__main__":
    main()
