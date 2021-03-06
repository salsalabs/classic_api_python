import csv
import datetime
import json
import logging
import os
import os.path
import pathlib
import threading

from base import ReaderBase, SaverBase

class DonationReader (ReaderBase):
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
        ReaderBase.__init__(self, **kwargs)

        x = []
        for k, v in DonationMap.items():
            if v:
                x.append(v)
        self.incl = ",".join(x)

    def run(self):
        """
        Run the thread.  overrides Thread.run().
        """

        self.log.info('starting')
        self.process_data()
        self.log.info('ending')

    def process_data(self):
        """
        Read supporters from the supporter queue.  Retrieve donation records
        for the supporters.  If a supporter is not active, then write the record
        to the downstream supporter saver.
        """

        count = 0
        while not self.exitFlag:
            supporter = self.donationQueue.get()
            if not supporter:
                continue
            if self.donationQueue.qsize() - count > 100:
                count = self.donationQueue.qsize()
            offset = 0
            count = 500
            cond = f"supporter_KEY={supporter['supporter_KEY']}&condition=RESULT IN 0,-1"

            while count == 500:
                payload = {'json': True,
                           "limit": f"{offset},{count}",
                           'object': 'donation',
                           'condition': cond,
                           'include': self.incl}
                u = f"https://{self.cred['host']}/api/getObjects.sjs"
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
                                try:
                                    x = datetime.datetime.strptime(x, f)
                                    d[k] = x.strftime("%Y-%m-%dT%H:%M:%S")
                                except ValueError as e:
                                    self.log.warn(str(e))


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
                # self.log.info(r)
                self.writer.writerow(r)
                self.csvfile.flush()
                count = count + 1

            except UnicodeEncodeError:
                self.log.error(f"{self.threadName}_{self.threadID:02d}: UnicodeEncodeError on {r}")

DonationMap = {
    "supporter_KEY":    "supporter_KEY",
    "Email":            "Email",
    "donation_KEY":     "donation_KEY",
    "Transaction_Date": "Transaction_Date",
    "Amount":           "amount",
    "Transaction_Type": "Transaction_Type"}
