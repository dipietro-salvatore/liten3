import os


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
            print("\n#####################################################")
            print(  "# Running in DRY RUN mode. No files will be deleted #")
            print(  "#####################################################\n")
        print("""
\t LITEN 3 \n

Starting a new Interactive Session.

* Duplicate files will be presented in numbered groups.
* Type one number at a time
* Hit Enter to skip to the next group.
* Ctrl-C cancels the operation, nothing will be deleted.
* Confirm your selections at the end.\n
-------------------------------------------------------\n""")

        for_deletion = list()

        try:
            for r in self.DB.getDuplicatesWithFilesAndHashes(paths).fetchall():
                hash = self.DB.getDuplicate(r['hashesid'], paths).fetchall()
                if len(hash) > 1:
                    filepaths = list([""])
                    count = 1
                    for i in hash :
                        filepaths.append(i['path'])
                        if not self.autoDelete:
                            print("%d \t %s" % (count, i['path']))
                            count += 1
                    if self.autoDelete :
                        files = self.are_files_in_folder(filepaths)
                        if files is not None:
                            for_deletion = for_deletion + files

                if not self.autoDelete:
                    try:
                        answer = True
                        while answer:
                            choose = int(input("Choose a number to delete (Enter to skip): "))
                            if filepaths[choose] not in for_deletion:
                                for_deletion.append(filepaths[choose])
                            if not choose:
                                answer = False

                    except ValueError:
                        print("--------------------------------------------------\n")

            print("Files selected for complete removal:\n")
            for selection in for_deletion:
                if selection:
                    print(selection)
            print("\n")

            if self.dryrun:
                print("###########################")
                print("# DRY RUN mode ends here. #")
                print("###########################\n")

            if not self.dryrun:
                confirm = input("Type Yes to confirm (No to cancel): ")
                if confirm in ["Yes", "yes", "Y", "y"]:
                    for selection in for_deletion:
                        if selection:
                            try:
                                print("Removing file:\t %s" % selection)
                                os.remove(selection)
                            except OSError:
                                "Could not delete:\t %s \nCheck Permissions" % selection
            else:
                print("Cancelling operation, no files were deleted.")

        except KeyboardInterrupt:
            print("\nExiting nicely from interactive mode. No files deleted\n")


    def delete(self, deletePaths):
        self.autoDelete = True
        self.deletePaths = deletePaths

    def are_files_in_folder(self, filesPath):
        founded = list()
        toDelete = list()

        for file in filesPath:
            if file != "": #not empty content
                foundedInFolders = list()
                for folder in self.deletePaths:
                    if file.startswith(folder):
                        toDelete.append(file)
                        foundedInFolders.append(True)
                    else:
                        foundedInFolders.append(False)

                if True in foundedInFolders:
                    founded.append(True)
                else:
                    founded.append(False)

        #If there is a FALSE at least one copy remains in the system
        if False in founded:
            return toDelete
        else:
            return None

