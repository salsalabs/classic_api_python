import csv
import threading


class SupporterReader (threading.Thread):
    # Read supporter records at a nominal rate of500 per chunk. Output them
    # singly to the output queues.

    def __init__(self, threadID, cred, session, cond, out1, out2, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.cred = cred
        self.session = session
        self.cond = cond
        self.out1 = out1
        self.out2 = out2
        self.exitFlag = exitFlag
        self.threadName = "drive"

    def run(self):
        print("Starting " + self.threadName)
        self.process_data()
        print("Ending  " + self.threadName)

    def process_data(self):
        # Read supporters from the database.  Queue them up individually
        # to the output queue(s).
        offset = 0
        count = 500
        while count == 500:
            payload = {'json': True,
                       "limit": "%d,%d" % (offset, count),
                       'object': 'supporter',
                       'condition': self.cond,
                       'include': 'supporter_KEY,First_Name,Last_Name,Email'}
            u = 'https://' + self.cred['host'] + '/api/getObjects.sjs'
            print("%s_%02d: reading %d from %7d" % (self.threadName, self.threadID, count, offset))
            r = self.session.get(u, params=payload)
            j = r.json()

            # Iterate through the hashes and push them onto the supporter
            # queue.
            for supporter in j:
                self.out1.put(supporter)
                self.out2.put(supporter)
                # print("%s: %s" % (self.threadName, supporter["supporter_KEY"]))

            count = len(j)
            offset += count


class SupporterSaver (threading.Thread):
    # Accepts supporter recvords from a queue and writes them to a CSV file.

    def __init__(self, threadID, inbound, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "saveSupporters"
        self.inbound = inbound
        self.exitFlag = exitFlag

        fn = "supporters_%02d.csv" % self.threadID
        self.csvfile = open(fn, "w")
        fieldnames = str.split("supporter_KEY,First_Name,Last_Name,Email", ",")
        self.writer = csv.DictWriter(self.csvfile, fieldnames=fieldnames)
        self.writer.writeheader()

    def run(self):
        print("Starting " + self.threadName)
        self.process_data()
        self.csvfile.flush()
        self.csvfile.close()
        print("Ending  " + self.threadName)

    def process_data(self):
        # Accept a (group_name, email) record from the queue.  Write it
        # as a record to a CSV file.
        while not self.exitFlag:
            supporter = self.inbound.get()
            if not supporter:
                continue
            # csv writer complains if there's stuff in the record
            # that's not going to be written
            del supporter['object']
            del supporter['key']
            self.writer.writerow(supporter)
