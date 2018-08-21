import csv
import threading


class SupporterReader (threading.Thread):
    # Read supporter records at a nominal rate of500 per chunk. Output them
    # singly to the output queues.

    def __init__(self, threadID, cred, session, cond, out1, out2, out3, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.cred = cred
        self.session = session
        self.cond = cond
        self.out1 = out1
        self.out2 = out2
        self.out3 = out3
        self.exitFlag = exitFlag
        self.threadName = "SupporterReader"
        x = []
        for k, v in SupporterMap.items():
            if v :
                x.append(v)
        self.incl = ",".join(x)
    def run(self):
        print(("Starting " + self.threadName))
        self.process_data()
        print(("Ending  " + self.threadName))

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
                       'include': self.incl}
            u = 'https://' + self.cred['host'] + '/api/getObjects.sjs'
            print(("%s_%02d: reading %d from %7d" % (self.threadName, self.threadID, count, offset)))
            r = self.session.get(u, params=payload)
            j = r.json()

            # Iterate through the records and push each onto the output queues.
            for supporter in j:
                self.out1.put(supporter)
                self.out2.put(supporter)
                self.out3.put(supporter)

            count = len(j)
            offset += count


class SupporterSaver (threading.Thread):
    # Accepts supporter recvords from a queue and writes them to a CSV file.

    def __init__(self, threadID, inbound, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "SupporterSaver"
        self.inbound = inbound
        self.exitFlag = exitFlag

        fn = "supporters_%02d.csv" % self.threadID
        self.csvfile = open(fn, "w")
        fieldnames = []
        for k, v in SupporterMap.items():
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
            # Classic-to-Engage fixes.
            if supporter['Receive_Email'] > '0':
                supporter['Receive_Email'] = "Subscribed"
            else:
                supporter['Receive_Email'] = "Unsubscribed"

            # Create a new dict of Engage headers and Classic values.
            m = {}
            for k, v in SupporterMap.items():
                if v:
                    m[k] = str.strip(supporter[v])
            try:
                self.writer.writerow(m)
            except UnicodeEncodeError:
                    print(("%s_%02d: UnicodeEncodeError on %s", self.threadName, self.threadID, supporter))

SupporterMap = {
    "Address, Line 1": "Street",
    "Address, Line 2": "Street_2",
    "Cell Phone": "Cell_Phone",
    "City": "City",
    "Country": "Country", 
    "Date of Birth": None,
    "Email Address": "Email",
    "Subscription Status": "Receive_Email",
    "External ID": "supporter_KEY",
    "Facebook ID": None,
    "First Name": "First_Name",
    "Last Name": "Last_Name",
    "Gender": None,
    "Home Phone": "Phone",
    "Middle Name": "MI",
    "Preferred Language": "Language_Code",
    "State": "State",
    "Suffix": "Suffix",
    "Timezone": "Timezone",
    "Title": "Title",
    "Twitter ID": None,
    "Work Phone": "Work_Phone",
    "Zip Code": "Zip"}
