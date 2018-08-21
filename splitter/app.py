
# Application to read all supporters, filter out the ones that don't need to be
# exported, then export the rest as a CSV.  This app also exports donations, groups,
# actions and events for the supporters.
import argparse
import requests
import time
import yaml

from groups import GroupsReader, GroupEmailSaver
from queue import LockedQueue
from salsa import Authenticate
from supporter import SupporterReader, SupporterSaver


def main():
    # The app starts here.
    exitFlag = False
    supporterQueue = LockedQueue()
    groupsQueue = LockedQueue()
    groupsEmailQueue = LockedQueue()
    threads = []
    threadID = 1

    # Get the login credentials
    parser = argparse.ArgumentParser(description='Read supporters')
    parser.add_argument('--login', dest='loginFile', action='store',
                        help='YAML file with login credentials')

    args = parser.parse_args()
    # Session to use for network I/O with automatic cookies.
    session = requests.Session()
    # Get the login credntials from a file.
    cred = yaml.load(open(args.loginFile))
    # Login.  Die if the crednentials are wrong.
    Authenticate(cred, session)

    t = GroupEmailSaver(threadID, groupsEmailQueue, exitFlag)
    t.start()
    threads.append(t)
    threadID += 1

    t = SupporterSaver(threadID, supporterQueue, exitFlag)
    t.start()
    threads.append(t)
    threadID += 1

    t = GroupsReader(threadID, cred, session, groupsQueue,
                     groupsEmailQueue, exitFlag)
    t.start()
    threads.append(t)
    threadID += 1

    cond = 'Email IS NOT EMPTY&condition=EMAIL LIKE %@%.%&condition=Receive_Email>0'
    t = SupporterReader(threadID, cred, session, cond,
                        supporterQueue, groupsQueue, exitFlag)
    t.start()
    threads.append(t)
    threadID += 1

    # Wait for all threads to complete
    for t in threads:
        t.join()
    print "Exiting Main Thread"


if __name__ == "__main__":
    main()
