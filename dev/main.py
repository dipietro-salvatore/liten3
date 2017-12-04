#!/usr/bin/env python
#Liten3 - Deduplication command line tool
#Author: Salvatore Dipietro (Initial Development by Noah Gift and Alfredo Deza)
#License: GPLv3 License
#Email: dipietro.salvatore [at] gmail dot com

__version__ = "3.0.1"
__date__ = "2017-11-20"


"""
Liten3 walks through a given path and creates a Checksum based on
the Sha512 library storing all the data in a Sqlite3 database.
You can run different kinds of reports once this has been done,
which gives you the ability to determine what files to delete.
"""


import sys
import os
import pathlib
import sqlite3
import time
import hashlib
from optparse import Option, OptionParser
from progressbar import Bar, Percentage, ProgressBar
from multiprocessing.pool import ThreadPool
from multiprocessing import Queue, cpu_count
import hashlib

from DB import DbWork
from Report import Report
from Files import Files
from Hashes import Hashes
from Interactive import Interactive

debug = False










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
    p.add_option('--dryrun', '-d', action="store_true", help="Does not delete anything. Use ONLY with interactive mode")
    p.add_option('--report', '-r', action="store_true", help="Generates a report from a previous run")
    p.add_option('--interactive', '-i', action="store_true", help='Interactive mode to delete files')
    p.add_option('--searchduplicate', '-D', action="store_true", help='Search duplicate files only')
    options, arguments = p.parse_args()



    """ Test options"""
    if options.open and options.tmpDB:
        sys.stderr.write("Do you want to use a temporary database (--tmpDB) or not?")
        sys.exit(1)



    """ Setting  """
    #SIZE
    if options.size:
        options.size = int(options.size) * 1048576
    else:
        options.size = 0

    #MaxSizeHash
    if options.maxsizehash:
        options.maxsizehash = int(options.maxsizehash) * 1048576
    else:
        options.maxsizehash = 0

    #PATHS
    if options.path and len(options.path) > 0:
        absolutePath = list()
        for i in options.path:
            absolutePath.append(os.path.abspath(i))
        options.path = absolutePath
        options.searchduplicate = True

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

    #DB
    dbfilename = str(pathlib.Path.home()) + "/.liten3.sqlite"
    if options.open:  dbfilename = os.path.abspath(options.open)
    if options.tmpDB: dbfilename = ':memory:'
    DB = DbWork(dbfilename, options.threads)
    DB.debug = debug



    """ Start program """
    if options.path:
        options.report = options.path
        for path in options.path:
            if not os.path.isdir(path):
                sys.stderr.write("Search path does not exist or is not a directory: %s\n" % path)
                sys.exit(1)

        for path in options.path:
            try:
                filesClass = Files(path, options.threads, options.size)
                filesClass.debug = debug
                DB.setIsUpdated(path)
                filesDetailsQueue = filesClass.findFiles()
                DB.insertFiles(path, filesDetailsQueue, options.hashall)

            except (KeyboardInterrupt, SystemExit):
                print("\nExiting nicely from Liten3...")
                sys.exit(1)

        DB.rmOldFiles(options.path)
        hashesClass = Hashes(options.threads)
        DB.findFilesWithSameSize(options.path, hashesClass, options.hashall)

        hashesQueue = hashesClass.calcHashes(options.maxsizehash)
        DB.insertHashes(hashesQueue, None)

    if options.searchduplicate:
        if not options.path: options.path = "/"
        DB.findAndInsertDuplicates(options.path)


    if options.report:
        reportClass = Report(DB)
        if not options.path: options.path = "/"
        if options.export:
            reportClass.full_report(options.path, options.export)
        else:
            reportClass.full_report(options.path, None)


    if options.interactive or options.delete:
        if options.dryrun:
            interactiveClass = Interactive(DB, dryrun=True)
        else:
            interactiveClass = Interactive(DB)
        interactiveClass.debug = debug

        if options.delete: interactiveClass.delete(options.delete)
        if not options.path:
            interactiveClass.session("%")
        else:
            interactiveClass.session(options.path)



    """ EXIT """
    DB.close()
    sys.exit(0)


if __name__=="__main__":
    main()
