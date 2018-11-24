import csv
import threading
import os
import os.path
import pathlib
import threading


class SupporterReader (threading.Thread):
    """
    Read supporter records at a nominal rate of 500 per chunk. Output them
    singly to the output queues.
    """

    def __init__(self, **kwargs): #threadID, cred, session, cond, supporterSaveQueue, groupQ, donationQueue, start, exitFlag):
        """
        Initialize a SupporterReader object
        
        Params in kwargs:
        
        :threadID: numeric cardinal thread identifier
        :cred:     login credentials (from the YAML file)
        :session:  requests object to read from Salsa
        :cond:     conditions to use to filter supporters
        :supporterSaveQueue: supporter save queue
        :groupQ:   group save queue
        :donationQueue:     donation sqve queue
        :start:    starting offset in the Salsa database
        :exitFlag: boolean indicating when processing should stop
        """

        threading.Thread.__init__(self)
        self.__dict__.update(kwargs)
        self.threadName = "SupporterReader"
        x = []
        for k, v in SupporterMap.items():
            if v :
                x.append(v)
        self.incl = ",".join(x)

    def run(self):
        """
        Run the thread.  Overrides Threading.run()
        """

        print("Running " + self.threadName)
        self.process_data()
        print("Ending  " + self.threadName)

    def process_data(self):
        """
        Read supporters from the database.  Queue them up individually
        to the output queue(s).
        """

        offset = self.offset
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
                if supporter["Receive_Email"] != "Unsubscribed":
                    self.supporterSaveQueue.put(supporter)
                    self.donationQueue.put(supporter)
                # Donations processor will write donation records for all supporters
                # with emails.  Donations will queue up supporter records for 
                # supporters with donation history.
                self.groupQ.put(supporter)

            count = len(j)
            offset += count


class SupporterSaver (threading.Thread):
    """
    Accepts supporter recvords from a queue and writes them to a CSV file.
    """

    def __init__(self, threadID, supporterSaveQueue, outDir, exitFlag):
        """
        Initialize a SupporterSaver object
        
        Params:
        
        :threadID: numeric cardinal thread ID
        :supporterSaveQueue: supporter queue
        :outDir:   directory where the CSV file(s) wil be stored
        :exitFlag: boolean indicating the processing is complete
        """

        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "SupporterSaver"
        self.supporterSaveQueue = supporterSaveQueue
        self.outDir = outDir
        self.exitFlag = exitFlag
        self.csvfile = None
        self.maxRecs = 50000
        self.fileRoot = "supporters"
        self.fileNum = 1

    def openFile(self):
        """
        Open a new CSV output file.  The filename contains the thread ID
        and the current file serial number.
        """

        while True:
            fn = "%s_%02d_%02d.csv" % (self.fileRoot, self.threadID, self.fileNum)
            fn = os.path.join(self.outDir, fn)
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
        for k, v in SupporterMap.items():
            if v:
                fieldnames.append(k)
        self.writer = csv.DictWriter(self.csvfile, fieldnames=fieldnames)
        self.writer.writeheader()

    def run(self):
        """
        Run the process.  Overrides Threading.run()
        """
    
        print(("Starting:" + self.threadName))
        self.process_data()
        self.csvfile.flush()
        self.csvfile.close()
        print(("Ending  " + self.threadName))

    def process_data(self):
        """
        Accept a supporter from the supporter save queue and save it to a CSV
        file.
        """

        count = self.maxRecs
        while not self.exitFlag:
            supporter = self.supporterSaveQueue.get()
            if not supporter:
                continue
            if count >= self.maxRecs:
                count = 0
                self.openFile()
            # csv writer complains if there's stuff in the record
            # that's not going to be written
            if 'object' in supporter:
                del supporter['object']
            if 'key' in supporter:
                del supporter['key']
            # Classic-to-Engage fixes.
            if supporter['Receive_Email'] > '0':
                supporter['Receive_Email'] = "Subscribed"
            else:
                supporter['Receive_Email'] = "Unsubscribed"

            # Create a new dict of Engage headers and Classic values.
            m = {}
            for k in SupportMapOrder:
                v = SupporterMap[k]
                if v:
                    m[k] = str.strip(supporter[v])
            try:
                self.writer.writerow(m)
                count = count + 1
            except UnicodeEncodeError:
                    print(("%s_%02d: UnicodeEncodeError on %s", self.threadName, self.threadID, supporter))

SupporterMap = {
    "AddressLine1": "Street",
    "AddressLine2": "Street_2",
    "CellPhone": "Cell_Phone",
    "City": "City",
    "Country": "Country", 
    "DateOfBirth": None,
    "Email": "Email",
    "SubscriptionStatus": "Receive_Email",
    "ExternalID": "supporter_KEY",
    "FacebookID": None,
    "FirstName": "First_Name",
    "LastName": "Last_Name",
    "Gender": None,
    "HomePhone": "Phone",
    "MiddleName": "MI",
    "PostalCode": "Zip",
    "PreferredLanguage": "Language_Code",
    "State": "State",
    "Suffix": "Suffix",
    "Timezone": "Timezone",
    "Title": "Title",
    "TwitterID": None,
    "WorkPhone": "Work_Phone"}

SupportMapOrder = [
    "ExternalID",
    "Email",
    "Title",
    "FirstName",
    "MiddleName",
    "LastName",
    "Suffix",
    "AddressLine1",
    "AddressLine2",
    "City",
    "State",
    "Country", 
    "PostalCode",
    "CellPhone",
    "WorkPhone",
    "SubscriptionStatus",
    "DateOfBirth",
    "FacebookID",
    "Gender",
    "HomePhone",
    "PreferredLanguage",
    "Timezone",
    "TwitterID"
]