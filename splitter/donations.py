import csv
import re
import threading


class DonationReader (threading.Thread):
    # Read supporters from a queue, find the groups that the supporter belongs
    # to, then write individual (group_Name, email) records to the output
    # queue

    def __init__(self, threadID, cred, session, supQ, out, exitFlag):
        threading.Thread.__init__(self)
        self.cred = cred
        self.session = session
        self.threadID = threadID
        self.supQ = supQ
        self.out = out
        self.exitFlag = exitFlag
        self.threadName = "DonationReader"
        x = []
        for k, v in SupporterMap.items():
            x.append(v)
        self.incl = ",".join(x)

    def run(self):
        print(("Starting " + self.threadName))
        self.process_data()
        print(("Ending   " + self.threadName))

    def process_data(self):
        while not self.exitFlag:
            supporter = self.supQ.get()
            if not supporter:
                continue
            offset = 0
            count = 500
            while count == 500:
                cond = "supporter_KEY=%s&condition=RESULT IN 0,-1" % supporter["supporter_KEY"]
                payload = {'json': True,
                           "limit": "%d,%d" % (offset, count),
                           'object': 'donation',
                           'condition': cond,
                           'include': self.incl}
                u = 'https://' + self.cred['host'] + '/api/getLeftJoin.sjs'
                r = self.session.get(u, params=payload)
                j = r.json()

                # Iterate through the donations and put them onto the donation saver
                # queue.
                for donation in j:
                    d = {}
                    for k, v in DonationMap.items():
                        if k == "supporter_KEY" or k == "Email":
                            d[k] = supporter[k]
                        else:
                            d[k] = donation[v]
                            if k == "Transaction_Type":
                                if d[k] != "Recurring":
                                    d[k] = "OneTime"
                    self.out.put(d)

                count = len(j)
                offset += count


class DonationSaver (threading.Thread):
    # Accepts (groupName, email) recvords from a queue and writes them to
    # a CSV file.

    def __init__(self, threadID, supQ, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "DonationSaver"
        self.supQ = supQ
        self.exitFlag = exitFlag

        fn = "donations_%02d.csv" % self.threadID
        self.csvfile = open(fn, "w")
        fieldnames = str.split("Group,Email", ",")
        self.writer = csv.DictWriter(self.csvfile, fieldnames=fieldnames)
        self.writer.writeheader()

    def run(self):
        print(("Starting " + self.threadName))
        self.process_data()
        self.csvfile.flush()
        self.csvfile.close()
        print(("Ending  " + self.threadName))

    def process_data(self):
        # f = '{:10}{:10} {:10} {:20}'
        while not self.exitFlag:
            r = self.supQ.get()
            try:
                self.writer.writerow(r)
            except UnicodeEncodeError:
                print(("%s_%02d: UnicodeEncodeError on %s",
                       self.threadName, self.threadID, r))

DonationMap = {
    "Supporter_KEY": None,
    "Email": None,
    "donation_KEY": "donation_KEY",
    "Transaction_Date": "Transaction_Date",
    "Amount": "amount",
    "Transaction_Type": "Transaction_Type"}
