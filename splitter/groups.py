import csv
import os
import os.path
import pathlib
import threading


class GroupsReader (threading.Thread):
    """
    Read supporters from a queue, find the groups that the supporter belongs
    to, then write individual (group_Name, email) records to the groupsEmailQueueput queue.
    """

    def __init__(self, **kwargs):


        """
        Initialize a DonationReader instance.
        
        Parameters in kwargs:

        :threadID:      numeric, cardinal thread identifier
        :cred:          login credentials (from the YAML file)
        :session:       requests session used to read from Salsa
        :groupsQueue:   queue to read to retrieve supporters
        :groupsEmailQueue:    queue to write to read and save groups
        :exitFlag:      boolean flag to indicate that processing has completed
        """

        threading.Thread.__init__(self)
        self.__dict__.update(kwargs)
        self.threadName = type(self).__name__

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
            supporter = self.groupsQueue.get()
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
                        self.groupsEmailQueue.put(r)

                count = len(j)
                offset += count


class GroupSaver (threading.Thread):
    """
    Accepts (groupName, email) records from a queue and writes them to
    to CSV file(s).
    """

    def __init__(self, **kwargs):
        """
        Initialize a GroupSaver.

        Params:

        :threadID:  numeric cardinal thread identifier
        :groupsEmailQueue: queue to read (groupName, email) records
        :outputDir: where the CSV file(s) wil end up
        :extFiag: boolean indicating the end of processing
        """

        threading.Thread.__init__(self)
        self.__dict__.update(kwargs)

        self.threadName = type(self).__name__
        self.fileRoot = self.threadName.replace("Saver", "").replace("Reader","" ).lower()
        self.csvfile = None
        self.maxRecs = 50000
        self.fileNum = 1

    def openFile(self):
        """
        Open a new CSV file.  The filename contains the thread ID and
        a current file serial number.
        """

        while True:
            fn = "%s_%02d_%02d.csv" % (self.fileRoot, self.threadID, self.fileNum)
            fn = os.path.join(self.outputDir, fn)
            self.fileNum = self.fileNum + 1
            p = pathlib.Path(fn)
            if not p.is_file():
                break

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
            r = self.groupsEmailQueue.get()
            if r and r["Group"] and r["Email"]:
                try:
                    if count >= self.maxRecs:
                        count = 0
                        self.openFile()
                    d = {}
                    for k, v in GroupsMap.items():
                        d[k] = r[v]

                    self.writer.writerow(d)
                    count = count + 1

                except UnicodeEncodeError:
                    print(("%s_%02d: UnicodeEncodeError on %s", self.threadName, self.threadID, r))

GroupsMap = {
    "Group": "Group",
    "Email": "Email"
}
