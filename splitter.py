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

class driveThread (threading.Thread):
    def __init__(self, threadID, s, cond, qOut, qOutLock, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "drive"
        self.queue=qOut
        self.queueLock = qOutLock
        self.exitFlag = exitFlag
    def run(self):
        print("Starting " + self.threadName)
        self.process_data()
        print("Ending  " + self.threadName)
        self.exitFlag = True
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
            r = s.get(u, params=payload)

            # This automatcally converts the JSON buffer to an array of dictionaries.
            j = r.json()

            # Iterate through the hashes and push them onto the supporter queue.
            for supporter in j:
                self.queueLock.acquire()
                self.queue.put(supporter)
                self.queueLock.release()
                print("%s: %s" % (self.threadName, supporter["supporter_KEY"]))

            count = 0 # len(a)
            offset = offset + count

class suppThread (threading.Thread):
    def __init__(self, threadID, q, qLock, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = "supp"
        self.queue=q
        self.queueLock = qLock
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
        with open('supporters.csv', 'w') as csvfile:
            f = '{:10}{:10} {:10} {:20}'
            while not self.exitFlag:
                self.queueLock.acquire()
                if not self.queue.empty():
                    supporter = self.queue.get()
                    self.queueLock.release()
                    # csv writer complains if there's stuff that's not going to be written
                    del supporter['object']
                    del supporter['key']
                    # print(f.format(supporter["supporter_KEY"],
                    #     supporter["First_Name"],
                    #     supporter["Last_Name"],
                    #     supporter["Email"]
                    #     ))
                    self.writer.writerow(supporter)
                else:
                    self.queueLock.release()

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

supporterQueue = Queue.Queue(100)
supporterQueueLock = threading.Lock()
threads = []
threadID = 1

# Get the login credentials
parser = argparse.ArgumentParser(description='Read supporters')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')
cond = 'Email IS NOT EMPTY&condition=EMAIL LIKE %@%.%&condition=Receive_Email>0'

args = parser.parse_args()
cred = yaml.load(open(args.loginFile))
s = requests.Session()
authenticate(cred, s)
# sample(cred, s)

t = suppThread(threadID, supporterQueue, supporterQueueLock, exitFlag)
t.start()
threads.append(t)
threadID += 1

t = suppThread(threadID, supporterQueue, supporterQueueLock, exitFlag)
t.start()
threads.append(t)
threadID += 1

t = driveThread(threadID, s, cond, supporterQueue, supporterQueueLock, exitFlag)
t.start()
threads.append(t)
threadID += 1

# Wait for all threads to complete
for t in threads:
    t.join()
print "Exiting Main Thread"

