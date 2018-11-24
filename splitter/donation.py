import csv
import datetime
import json
import os
import os.path
import pathlib
import threading

from saver_base import SaverBase

class DonationReader (threading.Thread):
    """Read supporters from a queue, find the donations for the supporters, 
    then write donation records records to the output queue."""

    def __init__(self, **kwargs):
        """
        Initialize a DonationReader instance.
        
        Parameters in kwargs:
        
        :threadID:      thread number, generally cardinal
        :cred:          login credentials (from the YAML file)
        :session:       requests session to use to read from Salsa
        :donationQueue: supporter queue.  Reads this to get supporters
        :donationSaveQueue:      donation request queue
        :supporterSaveQueue:      queue to receive inactive supporters with donation history
        :exitFlag:      boolean that's true when processing should stop
        """

        threading.Thread.__init__(self)
        self.__dict__.update(kwargs)
        self.threadName = type(self).__name__

        x = []
        for k, v in DonationMap.items():
            if v:
                x.append(v)
        self.incl = ",".join(x)

    def run(self):
        """
        Run the thread.  overrides Thread.run().
        """

        print(("Starting " + self.threadName))
        self.process_data()
        print(("Ending   " + self.threadName))

    def process_data(self):
        """
        Read supporters from the supporter queue.  Retrieve donation records
        for the supporters.  If a supporter is not active, then write the record
        to the downstream supporter saver. 
        """

        while not self.exitFlag:
            supporter = self.donationQueue.get()
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

                # Supporter had donations.  If this supporter is no longer active,
                # then send the supporter record to the supporter save queue.
                if supporter['Receive_Email'] == "Unsubscribed":
                    self.supporterSaveQueue.put(supporter)
    
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
                    self.donationSaveQueue.put(d)


class DonationSaver (SaverBase):
    """
    Accepts donation records from a queue, then write them to a CSV file.
    """

    def __init__(self, **kwargs):
        """
        Initialize a DonationSaver instance
        """
        SaverBase.__init__(self, **kwargs)

    def getFieldMap(self):
        """
        Specify the output fields for the CSV file.  Overrides SaverBase.getFieldMap();
        """
        return DonationMap

    def process_data(self):
        """
        Read donations from the donation queue, format them, then write them to
        the CSV file.  Overrides SaverBase.process_data to accept and save
        donations.
        """

        count = self.maxRecs
        while not self.exitFlag:
            r = self.donationSaveQueue.get()
            if not r:
                continue
            try:
                if count >= self.maxRecs:
                    count = 0
                    self.openFile()
                # print(r)
                self.writer.writerow(r)
                self.csvfile.flush()
                count = count + 1

            except UnicodeEncodeError:
                print(("%s_%02d: UnicodeEncodeError on %s",
                       self.threadName, self.threadID, r))

DonationMap = {
    "supporter_KEY":    "supporter_KEY",
    "Email":            "Email",
    "donation_KEY":     "donation_KEY",
    "Transaction_Date": "Transaction_Date",
    "Amount":           "amount",
    "Transaction_Type": "Transaction_Type"}
