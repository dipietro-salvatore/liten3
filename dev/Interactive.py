import os, logging


class Interactive(object):
    """This mode creates a session to delete files"""

    def __init__(self,DB, dryrun=False):
        self.DB = DB
        self.dryrun = dryrun
        self.debug = False
        self.autoDelete = False


    # def setDebug(self, debug=False):
    #     self.debug=debug

    def session(self, paths="%"):
        "starts a new session"

        if self.dryrun:
            logging.info("\n#####################################################")
            logging.info("# Running in DRY RUN mode. No files will be deleted #")
            logging.info("#####################################################\n")
        logging.info("""
\t LITEN 3 \n

Starting a new Interactive Session.

* Duplicate files will be presented in numbered groups.
* Type one number at a time
* Hit Enter to skip to the next group.
* Ctrl-C cancels the operation, nothing will be deleted.
* Confirm your selections at the end.\n
-------------------------------------------------------\n""")

        forDeletion = list()

        try:
            for r in self.DB.getDuplicatesWithFilesAndHashes(paths, groupBy="duplicates.hashesid").fetchall():
                hash = self.DB.getDuplicate(r['hashesid'], paths).fetchall()
                if len(hash) > 1:
                    filepaths = list([""])
                    count = 1
                    for i in hash :
                        filepaths.append(i['path'])
                        if not self.autoDelete:
                            logging.info("%d \t %s" % (count, i['path']))
                            count += 1
                    if self.autoDelete :
                        files = self.areFilesInFolder(filepaths)
                        if files is not None:
                            forDeletion = forDeletion + files

                if not self.autoDelete:
                    try:
                        answer = True
                        while answer:
                            choose = int(input("Choose a number to delete (Enter to skip): "))
                            if filepaths[choose] not in forDeletion:
                                forDeletion.append(filepaths[choose])
                            if not choose:
                                answer = False

                    except ValueError:
                        logging.info("--------------------------------------------------\n")

            logging.info("Files selected for complete removal:\n")
            for selection in forDeletion:
                if selection:
                    print(selection)
            logging.info("\n")

            if self.dryrun:
                logging.info("###########################")
                logging.info("# DRY RUN mode ends here. #")
                logging.info("###########################\n")

            if not self.dryrun:
                confirm = input("Type Yes to confirm (No to cancel): ")
                if confirm in ["Yes", "yes", "Y", "y"]:
                    for selection in forDeletion:
                        if selection:
                            try:
                                logging.info("Removing file:\t %s" % selection)
                                os.remove(selection)
                                self.DB.deleteFilePath(selection)
                            except OSError:
                                logging.error("Could not delete:\t %s \nCheck Permissions" % selection)
            else:
                logging.error("Cancelling operation, no files were deleted.")

        except KeyboardInterrupt:
            logging.error("\nExiting nicely from interactive mode. No files deleted\n")


    def delete(self, deletePaths):
        self.autoDelete = True
        self.deletePaths = deletePaths

    def areFilesInFolder(self, filesPath):
        toDelete = list()

        for file in filesPath:
            if file != "": #not empty content
                for folder in self.deletePaths:
                    if file.startswith(folder):
                        toDelete.append(file)

        #If there is a FALSE at least one copy remains in the system
        if len(toDelete) < len(filesPath):
            return toDelete
        else:
            return None

