
class Duplicates():

    def __init__(self):
        self.duplicateList = list()

    def insertOrUpdate(self, dbRecords, newDuplicate):
        # hashesid = dict()
        duplicateRecords = dict()
        duplicatesid = dict()

        for rec in dbRecords:
            if rec['hash'] not in duplicateRecords:
                duplicateRecords[rec['hash']] = list()
                # hashesid[rec['hashes.hash']] = rec['hashes.hashesid']
            duplicateRecords[rec['hash']].append(rec['filesid'])
            duplicatesid[rec['filesid']] = rec['duplicatesid']

        inserts = list()
        updates = list()
        for rec in newDuplicate:
            if rec['hash'] not in duplicateRecords or rec['filesid'] not in duplicateRecords[rec['hash']]:
                inserts.append({'hashesid':rec['hashesid'], 'filesid':rec['filesid']})
            else:
                updates.append(str(duplicatesid[rec['filesid']]))

        return inserts,updates
