#!/usr/bin/python

# Application to read all supporters, filter out the ones that don't need to be
# exported, then export the rest as a CSV.  This app also exports donations, groups,
# actions and events for the supporters.
import argparse
import csv
import json
import Queue
import requests
import threading
import time
import yaml

exitFlag = 0

class lockedQueue:
    # A queue and its guard lock.
    def __init__(self):
        # Initialize a queue and a lock.
        self.q = Queue.Queue()
        self.lock = threading.Lock()
    def get(self):
        # Lock the queue and retrieve an item.  Returns None
        # if the queue is empty.
        self.lock.acquire()
        p = None
        if not self.q.empty():
            p = self.q.get()
            self.lock.release()
        else:
            self.lock.release()
        return p
    def put(self, p):
        # Lock the queue and put an item on it.
        self.lock.acquire()
        self.q.put(p)
        self.lock.release()

class readSupporters (threading.Thread):
    # Read supporter records at a nominal rate of500 per chunk. Output them
    # singly to the output queues.
    def __init__(self, threadID, session, cond, out1, out2, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "drive"
        self.out1 = out1
        self.out2 = out2
        self.exitFlag = exitFlag
    def run(self):
        print("Starting " + self.threadName)
        self.process_data()
        print("Ending  " + self.threadName)
    def process_data(self):
        offset = 0
        count = 500
        while count == 500:
            payload = {'json': True,
                "limit": "%d,%d" % (offset, count),
                'object': 'supporter',
                'condition': cond,
                'include': 'supporter_KEY,First_Name,Last_Name,Email' }
            u = 'https://'+ cred['host'] +'/api/getObjects.sjs'
            r = session.get(u, params=payload)
            j = r.json()

            # Iterate through the hashes and push them onto the supporter queue.
            for supporter in j:
                self.out1.put(supporter)
                self.out2.put(supporter)
                print("%s: %s" % (self.threadName, supporter["supporter_KEY"]))

            count = 0 # len(j)
            offset = offset + count

class readGroups (threading.Thread):
    def __init__(self, session, threadID, in1, out, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "readGroups"
        self.in1 = in1
        self.out = out
        self.exitFlag = exitFlag
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
                cond = "&condition=supporter_KEY=%d" % (offset, count, supporter["supporter_KEY"])
                payload = {'json': True,
                    "limit": "%d,%d" % (offset, count),
                    'object': 'supporter_groups(groups_KEY)groups',
                    'condition': cond,
                    'include': 'groups.Group_Name' }
                u = 'https://'+ cred['host'] +'/api/getLeftJoin.sjs'
                r = session.get(u, params=payload)
                j = r.json()

                # Iterate through the groups and push them onto the (groups,email)_
                # queue.
                for group in j:
                    r = [ group.Group_Name, supporter.Email ]
                    self.out.put(r)
                    print("%s: %s" % (self.threadName, r))

            count = len(a)
            offset = offset + count

class saveSupporters (threading.Thread):
    # Accepts supporter recvords from a queue and writes them to a CSV file.
    def __init__(self, threadID, in1, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "saveSupporters"
        self.in1 = in1
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
        # f = '{:10}{:10} {:10} {:20}'
        while not self.exitFlag:
            supporter = self.in1.get()
            if not supporter:
                continue
            # csv writer complains if there's stuff in the record
            # that's not going to be written
            del supporter['object']
            del supporter['key']
            # print(f.format(supporter["supporter_KEY"],
            #     supporter["First_Name"],
            #     supporter["Last_Name"],
            #     supporter["Email"]
            #     ))
            self.writer.writerow(supporter)

class saveGroupEmail (threading.Thread):
        # Accepts (groupName, email) recvords from a queue and writes them to
        # a CSV file.
    def __init__(self, threadID, in1, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "ge"
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
                # csv writer complains if there's stuff in the record
                # that's not going to be written
                del supporter['object']
                del supporter['key']
                # print(f.format(supporter["supporter_KEY"],
                #     supporter["First_Name"],
                #     supporter["Last_Name"],
                #     supporter["Email"]
                #     ))
                self.writer.writerow(r)

def authenticate(cred, s):
    # Authenticate with Salsa.  Errors are fatal.
    payload = {
        'email': cred['email'],
        'password': cred['password'],
        'json': True }
    u = 'https://' + cred['host'] + '/api/authenticate.sjs'
    r = s.get(u, params=payload)
    j = r.json()
    if j['status'] == 'error':
        print('Authentication failed: ', j)
        exit(1)

    print('Authentication: ', j)

# Suppporters are read from the database and put onto this queue.
# Records on this queue are written the the supporter CSV file(s).
sq = lockedQueue()
# Supporters are read from the database and put onto this queue.
# Records on this queue are packaged as (group_name, email) records
# and put onto a queue.
grp = lockedQueue()
# Records on this queue are saved to (gruop_name, email) CSV files.
ge = lockedQueue()
# Thread management.
threads = []
threadID = 1

# Get the login credentials
parser = argparse.ArgumentParser(description='Read supporters')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')
cond = 'Email IS NOT EMPTY&condition=EMAIL LIKE %@%.%&condition=Receive_Email>0'

args = parser.parse_args()
# Session to use for network I/O with automatic cookies.
session = requests.Session()
# Get the login credntials from a file.
cred = yaml.load(open(args.loginFile))
# Login.  Die if the crednentials are wrong.
authenticate(cred, session)

t = saveGroupEmail(threadID, ge, exitFlag)
t.start()
threads.append(t)
threadID += 1

t = saveSupporters(threadID, sq, exitFlag)
t.start()
threads.append(t)
threadID += 1

t = readGroups(threadID, session, grp, ge, exitFlag)
t.start()
threads.append(t)
threadID += 1

t = readSupporters(threadID, session, cond, sq, grp, exitFlag)
t.start()
threads.append(t)
threadID += 1

# Wait for all threads to complete
for t in threads:
    t.join()
print "Exiting Main Thread"
