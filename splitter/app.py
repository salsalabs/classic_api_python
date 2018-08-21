
# Application to read all supporters, filter out the ones that don't need to be
# exported, then export the rest as a CSV.  This app also exports donations, groups,
# actions and events for the supporters.
import argparse
import requests
import time
import yaml

from groups import GroupsReader, GroupEmailSaver
from lckqueue import LockedQueue
from salsa import Authenticate
from supporter import SupporterReader, SupporterSaver


def main():
    # The app starts here.
    exitFlag = False
    supporterQueue = LockedQueue()
    groupsQueue = LockedQueue()
    groupsEmailQueue = LockedQueue()
    threads = []

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

    t = GroupEmailSaver(1, groupsEmailQueue, exitFlag)
    t.start()
    threads.append(t)

    t = SupporterSaver(1, supporterQueue, exitFlag)
    t.start()
    threads.append(t)

    t = GroupsReader(1, cred, session, groupsQueue,
                     groupsEmailQueue, exitFlag)
    t.start()
    threads.append(t)

    cond = 'Email IS NOT EMPTY&condition=EMAIL LIKE %@%.%&condition=Receive_Email>0'
    t = SupporterReader(1, cred, session, cond,
                        supporterQueue, groupsQueue, exitFlag)
    t.start()
    threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()
    print("Exiting Main Thread")


if __name__ == "__main__":
    main()
