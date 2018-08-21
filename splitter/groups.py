import csv
import re
import threading


class GroupsReader (threading.Thread):
    # Read supporters from a queue, find the groups that the supporter belongs
    # to, then write individual (group_Name, email) records to the output
    # queue

    def __init__(self, threadID, cred, session, in1, out, exitFlag):
        threading.Thread.__init__(self)
        self.cred = cred
        self.session = session
        self.threadID = threadID
        self.in1 = in1
        self.out = out
        self.exitFlag = exitFlag
        self.threadName = "readGroups"

    def run(self):
        print("Starting " + self.threadName)
        self.process_data()
        print("Ending   " + self.threadName)

    def process_data(self):
        while not self.exitFlag:
            supporter = self.in1.get()
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
                    r = { "Group": group["Group_Name"], "Email": supporter["Email"] }
                    self.out.put(r)

                count = len(j)
                offset += count


class GroupEmailSaver (threading.Thread):
    # Accepts (groupName, email) recvords from a queue and writes them to
    # a CSV file.

    def __init__(self, threadID, in1, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "GroupEmailSaver"
        self.in1 = in1
        self.exitFlag = exitFlag

        fn = "groups_%02d.csv" % self.threadID
        self.csvfile = open(fn, "w")
        fieldnames = str.split("Group,Email", ",")
        self.writer = csv.DictWriter(self.csvfile, fieldnames=fieldnames)
        self.writer.writeheader()

    def run(self):
        print("Starting " + self.threadName)
        self.process_data()
        self.csvfile.flush()
        self.csvfile.close()
        print("Ending  " + self.threadName)

    def process_data(self):
        # f = '{:10}{:10} {:10} {:20}'
        while not self.exitFlag:
            r = self.in1.get()
            if r:
                try:
                    self.writer.writerow(r)
                except UnicodeEncodeError:
                    print("%s_%02d: UnicodeEncodeError on %s", self.threadName, self.threadID, r)

