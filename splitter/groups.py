import csv
import os
import os.path
import threading


class GroupsReader (threading.Thread):
    """
    Read supporters from a queue, find the groups that the supporter belongs
    to, then write individual (group_Name, email) records to the groupSaveQput queue.
    """

    def __init__(self, threadID, cred, session, supQ, groupSaveQ, exitFlag):
        """
        Initialize a GroupsReader instance.

        Params:

        :threadID:   numeric, cardinal thread identifier
        :cred:       Salsa read criteria.  Does not include supporter_KEY.
        :session:    requests session used to read from Salsa
        :supQ:       queue to read to retrieve supporters
        :groupSaveQ: queue to write to read and save groups
        :exitFlag:   boolean flag to indicate that processing has completed
        """

        threading.Thread.__init__(self)
        self.cred = cred
        self.session = session
        self.threadID = threadID
        self.supQ = supQ
        self.groupSaveQ = groupSaveQ
        self.exitFlag = exitFlag
        self.threadName = "GroupsReader"

    def run(self):
        """
        Run the thread.  Overrides Threading.run()
        """

        print(("Starting " + self.threadName))
        self.process_data()
        print(("Ending   " + self.threadName))

    def process_data(self):
        """
        Read supporters and retrieve groups.  Push relevant group information
        onto the group save queue.
        """

        while not self.exitFlag:
            supporter = self.supQ.get()
            if not supporter:
                continue
            offset = 0
            count = 500
            while count == 500:
                cond = "supporter_KEY=%s&condition=Group_Name IS NOT EMPTY" % supporter["supporter_KEY"]
                payload = {'json': True,
                           "limit": "%d,%d" % (offset, count),
                           'object': 'supporter_groups(groups_KEY)groups',
                           'condition': cond,
                           'include': 'groups.Group_Name'}
                u = 'https://' + self.cred['host'] + '/api/getLeftJoin.sjs'
                r = self.session.get(u, params=payload)
                j = r.json()

                # Iterate through the groups and push them onto the (groups,email)_
                # queue.
                for group in j:
                    g = str.strip(group["Group_Name"])
                    e = str.strip(supporter["Email"])
                    if len(g) > 0 and len(e) > 0:
                        r = { "Group": g, "Email": e }
                        self.groupSaveQ.put(r)

                count = len(j)
                offset += count


class GroupEmailSaver (threading.Thread):
    """
    Accepts (groupName, email) records from a queue and writes them to
    to CSV file(s).
    """

    def __init__(self, threadID, groupSaveQ, outDir, exitFlag):
        """
        Initialize a GroupEmailSaver.

        Params:

        :threadID:  numeric cardinal thread identifier
        :groupSaveQ: queue to read (groupName, email) records
        :outDir: where the CSV file(s) wil end up
        :extFiag: boolean indicating the end of processing
        """

        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "GroupEmailSaver"
        self.groupSaveQ = groupSaveQ
        self.outDir = outDir
        self.exitFlag = exitFlag
        self.csvfile = None
        self.maxRecs = 50000
        self.fileNum = 1

    def openFile(self):
        """
        Open a new CSV file.  The filename contains the thread ID and
        a current file serial number.
        """

        fn = "groups_%02d_%02d.csv" % (self.threadID, self.fileNum)
        fn = os.path.join(self.outDir, fn)
        d = os.path.dirname(fn)
        if not os.path.exists(d):
            os.makedirs(d)
        if self.csvfile != None:
            self.csvfile.flush()
            self.csvfile.close()
        self.csvfile = open(fn, "w")
        fieldnames = []
        for k, v in GroupsMap.items():
            if v:
                fieldnames.append(k)
        self.writer = csv.DictWriter(self.csvfile, fieldnames=fieldnames)
        self.writer.writeheader()

    def run(self):
        """
        Run the thread.  Overrides Threading.run()
        """

        print(("Starting " + self.threadName))
        self.process_data()
        self.csvfile.flush()
        self.csvfile.close()
        print(("Ending  " + self.threadName))

    def process_data(self):
        """
        Read (groupName, email) records from a queue and save them to one or
        more CSV file(s).
        """

        count = self.maxRecs
        while not self.exitFlag:
            r = self.groupSaveQ.get()
            if r and r["Group"] and r["Email"]:
                try:
                    if count >= self.maxRecs:
                        count = 0
                        self.openFile()
                        self.fileNum = self.fileNum + 1
                    d = {}
                    for k, v in GroupsMap.items():
                        d[k] = r[v]

                    self.writer.writerow(d)
                except UnicodeEncodeError:
                    print(("%s_%02d: UnicodeEncodeError on %s", self.threadName, self.threadID, r))

GroupsMap = {
    "Group": "Group",
    "Email": "Email"
}
