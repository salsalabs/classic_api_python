import csv
import os
import logging
import os.path
import pathlib
import threading

from base import ReaderBase, SaverBase

class SupporterReader (ReaderBase):
    """
    Read supporter records at a nominal rate of 500 per chunk. Output them
    singly to the output queues.
    """

    def __init__(self, **kwargs):
        """
        Initialize a SupporterReader object

        Params in kwargs:

        :threadID: numeric cardinal thread identifier
        :cred:     login credentials (from the YAML file)
        :session:  requests object to read from Salsa
        :cond:     conditions to use to filter supporters
        :supporterSaveQueue: supporter save queue
        :groupsQueue:   group save queue
        :donationQueue:     donation sqve queue
        :start:    starting offset in the Salsa database
        :exitFlag: boolean indicating when processing should stop
        """
        ReaderBase.__init__(self, **kwargs)

        x = []
        for k, v in SupporterMap.items():
            if v :
                x.append(v)
        self.incl = ",".join(x)

    def run(self):
        """
        Run the thread.  Overrides Threading.run()
        """

        self.log.info('starting')
        self.process_data()
        self.log.info("Ending  " + self.threadName)

    def process_data(self):
        """
        Read supporters from the database.  Queue them up individually
        to the output queue(s).
        """
        offset = self.offset
        count = 500
        while count == 500:
            payload = {'json': True,
                       "limit": f"{offset},{count}",
                       'object': 'supporter',
                       'condition': self.cond,
                       'include': self.incl}
            u = f"https://{self.cred['host']}/api/getObjects.sjs"
            self.log.info(f"reading {count} from {offset:7d}")
            r = self.session.get(u, params=payload)
            j = r.json()

            # Iterate through the records and push each onto the output queues.
            for supporter in j:
                if supporter["Receive_Email"] != "Unsubscribed":
                    self.supporterSaveQueue.put(supporter)
                    # Donations processor will write donation records for all supporters
                    # with emails.  Donations will queue up supporter records for
                    # supporters with donation history.
                    self.donationQueue.put(supporter)
                self.groupsQueue.put(supporter)

            count = len(j)
            offset += count

class SupporterSaver (SaverBase):
    """
    Accepts Group records from a queue, then write them to a CSV file.
    """

    def __init__(self, **kwargs):
        """
        Initialize a GroupSaver instance
        """
        SaverBase.__init__(self, **kwargs)

    def getFieldMap(self):
        """
        Specify the output fields for the CSV file.  Overrides SaverBase.getFieldMap();
        """
        return SupporterMap

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
                v = self.getFieldMap()[k]
                if v:
                    m[k] = str.strip(supporter[v])
            try:
                self.writer.writerow(m)
                count = count + 1
            except UnicodeEncodeError:
                self.log.error(f"{self.threadName}_{self.threadID:02d}: UnicodeEncodeError on {supporter}")
# QZ 25-Jan-2019
SupporterMap = {
        "email":           "Email",
        "title":           "Title",
        "firstName":       "First_Name",
        "middleName":      "MI",
        "lastName":        "Last_Name",
        "suffix":          "Suffix",
        "status":          "Receive_Email",
        "addressLine1":    "Street",
        "addressLine2":    "Street_2",
        "city":            "City",
        "state":           "State",
        "country":         "Country",
        "postalCode":      "Zip",
        "homePhone":       "Phone",
        "cellPhone":       "Cell_Phone",
        "workPhone":       "Work_Phone",
        "languageCode":    "Language_Code",
        "externalID":      "supporter_KEY",
        "supporter_KEY":   "supporter_KEY",
        "Date_Created":    "Date_Created",
        "Last_Modified":   "Last_Modified"
}
SupportMapOrder = [
    "externalID",
    "email",
    "status",
    "title",
    "firstName",
    "middleName",
    "lastName",
    "suffix",
    "addressLine1",
    "addressLine2",
    "city",
    "state",
    "country",
    "postalCode",
    "homePhone",
    "cellPhone",
    "workPhone",
    "supporter_KEY",
    "Date_Created",
    "Last_Modified",
]
