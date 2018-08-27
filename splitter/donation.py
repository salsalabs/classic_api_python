import csv
import datetime
import json
import os
import os.path
import re
import threading



class DonationReader (threading.Thread):
    # Read supporters from a queue, find the donationsfor the supporters 
    # to, then write donation records records to the output queue.
    def __init__(self, threadID, cred, session, supQ, out, supSaveQueue, exitFlag):
        threading.Thread.__init__(self)
        self.cred = cred
        self.session = session
        self.threadID = threadID
        self.supQ = supQ
        self.out = out
        self.supSaveQueue = supSaveQueue
        self.exitFlag = exitFlag

        self.threadName = "DonationReader"
        x = []
        for k, v in DonationMap.items():
            if v:
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

                count = len(j)
                offset += count
                if count == 0:
                    continue

                # If this supporter is no longer active, then send the supporter
                # record to the supporter save queue.
                if supporter['Receive_Email'] == "Unsubscribed":
                    self.supSaveQueue.put(supporter)
    
                # Iterate through the donations, transmogrify as needed, then put them onto
                # the donation saver queue.
                for donation in j:
                    if not donation:
                        continue

                    d = {}
                    for k, v in DonationMap.items():
                        if k == "supporter_KEY" or k == "Email":
                            d[k] = supporter[k]
                        else:
                            d[k] = donation[v]
                            if k == "Transaction_Type":
                                if d[k] != "Recurring":
                                    d[k] = "OneTime"
                            if k == "Transaction_Date":
                                x = str.split(d[k], " GMT")[0]
                                f = "%a %b %d %Y %H:%M:%S"
                                x = datetime.datetime.strptime(x, f)
                                d[k] = x.strftime("%Y-%m-%dT%H:%M:%S")
                    self.out.put(d)


class DonationSaver (threading.Thread):
    # Accepts (groupName, email) recvords from a queue and writes them to
    # a CSV file.

    def __init__(self, threadID, supQ, outDir, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "DonationSaver"
        self.supQ = supQ
        self.outDir = outDir
        self.exitFlag = exitFlag
        self.csvfile = None
        self.maxRecs = 50000
        self.fileNum = 1

    def openFile(self):
        #Open a new CSV output file.  The filename contains the thread ID and
        #a current file serial number.
        fn = "supporters_%02d_%02d.csv" % (self.threadID, self.fileNum)
        fn = os.path.join(self.outDir, fn)
        d = os.path.dirname(fn)
        if not os.path.exists(d):
            os.makedirs(d)
        if self.csvfile != None:
            self.csvfile.flush()
            self.csvfile.close()
        self.csvfile = open(fn, "w")
        fieldnames = []
        for k, v in DonationMap.items():
            if v:
                fieldnames.append(k)
        self.writer = csv.DictWriter(self.csvfile, fieldnames=fieldnames)
        self.writer.writeheader()

    def run(self):
        print(("Starting " + self.threadName))
        self.process_data()
        self.csvfile.flush()
        self.csvfile.close()
        print(("Ending  " + self.threadName))

    def process_data(self):
        count = self.maxRecs
        while not self.exitFlag:
            r = self.supQ.get()
            if not r:
                continue
            try:
                if count >= self.maxRecs:
                    count = 0
                    self.openFile()
                    self.fileNum = self.fileNum + 1
                
                self.writer.writerow(r)
            except UnicodeEncodeError:
                print(("%s_%02d: UnicodeEncodeError on %s",
                       self.threadName, self.threadID, r))

# 'None' is a marker to tell this class that the fields need to come from
# the supporter record and not from the donation record.
DonationMap = {
    "supporter_KEY": "supporter_KEY",
    "Email": "Email",
    "donation_KEY": "donation_KEY",
    "Transaction_Date": "Transaction_Date",
    "Amount": "amount",
    "Transaction_Type": "Transaction_Type"}
