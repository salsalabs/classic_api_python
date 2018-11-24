import csv
import os
import os.path
import pathlib
import threading

from saver_base import SaverBase

class GroupsReader (threading.Thread):
    """
    Read supporters from a queue, find the groups that the supporter belongs
    to, then write individual (group_Name, email) records to the groupsEmailQueueput queue.
    """

    def __init__(self, **kwargs):

        """
        Initialize a GroupsReader instance.
        
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
            email = str.strip(supporter["Email"])
            if len(email) == 0:
                continue

            offset = 0
            count = 500
            while count == 500:
                cond = f"supporter_KEY={supporter['supporter_KEY']}&condition=Group_Name IS NOT EMPTY"
                payload = {'json': True,
                           "limit": f"{offset},{count}",
                           'object': 'supporter_groups(groups_KEY)groups',
                           'condition': cond,
                           'include': 'groups.Group_Name'}
                u = f"https://{self.cred['host']}/api/getLeftJoin.sjs"
                r = self.session.get(u, params=payload)
                j = r.json()

                # Iterate through the groups and push them onto the (groups,email)
                # queue.
                for group in j:
                    g = str.strip(group["Group_Name"])
                    if len(g) > 0:
                        r = { "Group": g, "Email": email }
                        self.groupsEmailQueue.put(r)

                count = len(j)
                offset += count

class GroupSaver (SaverBase):
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
        return GroupsMap

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
                    for k, v in self.getFieldMap().items():
                        d[k] = r[v]

                    self.writer.writerow(d)
                    count = count + 1

                except UnicodeEncodeError:
                    print(("%s_%02d: UnicodeEncodeError on %s", self.threadName, self.threadID, r))

GroupsMap = {
    "Group": "Group",
    "Email": "Email"
}
