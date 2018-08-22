"""Application to read all supporters, filter out the ones that don't need to be
# exported, then export the rest as a CSV.  This app also exports donations, groups,
# actions and events for the supporters."""

import argparse
import requests
import time
import yaml

from donation import DonationReader, DonationSaver
from groups import GroupsReader, GroupEmailSaver
from lckqueue import LockedQueue
from salsa import Authenticate
from supporter import SupporterReader, SupporterSaver


def main():
    """ The app starts a supporter reader and several listeners.  The reader
    retrieves qualified supporters from Salsa.  Each supporter is then passed
    to one of the first-tier listeners.
    
    * supporter listener -- accepts a supporter and writes to the supporter
      CSV file.
    groups listener - accepts a supporter and passes on (group_name, email)
      records to the second-tier groups-email listener
    * donation listener -- accepts a supporter and passes donation record to the
      second-tier donation-saver listener
      
    The second-tier listeners write to CSV files.ArithmeticError
    
    * groups-email listener -- accepts a (group_name, email) record and writes it 
      to the groups CSV file
    * donation save listener -- accepts a donation record and writes it to the 
      donation CSV file.

    The application starts from the command line.  The user provides a file
    containing Salsa campaign manager credentials.  The user may also set
    the directory where CSV files will be written.
    
     python3 splitter/app.py --help
    usage: app.py [-h] [--login LOGINFILE] [--dir OUTPUTDIR]
    
    Export supporters, groups and donations for export to Engage.
    
    optional arguments:
      -h, --help         show this help message and exit
      --login LOGINFILE  YAML file with login credentials
      --dir OUTPUTDIR    Store export files here
 
    Note:
    
    This application *will not* run with python2.  Use python3.
    """
    
    exitFlag = False
    supporterQueue = LockedQueue()
    groupsQueue = LockedQueue()
    groupsEmailQueue = LockedQueue()
    donationQueue = LockedQueue()
    donationSaveQueue = LockedQueue()
    threads = []

    # Get the login credentials
    parser = argparse.ArgumentParser(description='Export supporters, groups and donations for export to Engage.')
    parser.add_argument('--login', dest='loginFile', action='store',
                        help='YAML file with login credentials')
    parser.add_argument('--dir', dest='outputDir', action='store', default='./data',
                        help='Store export files here')


    args = parser.parse_args()
    # Session to use for network I/O with automatic cookies.
    session = requests.Session()
    # Get the login credntials from a file.
    cred = yaml.load(open(args.loginFile))
    # Login.  Die if the crednentials are wrong.
    Authenticate(cred, session)

    t = DonationSaver(1, donationSaveQueue, args.outputDir, exitFlag)
    t.start()
    threads.append(t)

    t = GroupEmailSaver(1, groupsEmailQueue, args.outputDir, exitFlag)
    t.start()
    threads.append(t)

    t = SupporterSaver(1, supporterQueue, args.outputDir, exitFlag)
    t.start()
    threads.append(t)

    t = DonationReader(1, cred, session, donationQueue, donationSaveQueue, exitFlag)
    t.start()
    threads.append(t)

    t = GroupsReader(1, cred, session, groupsQueue,
                     groupsEmailQueue, exitFlag)
    t.start()
    threads.append(t)

    cond = 'Email IS NOT EMPTY&condition=EMAIL LIKE %@%.%&condition=Receive_Email>0'
    t = SupporterReader(1, cred, session, cond,
                        supporterQueue, groupsQueue, donationQueue, exitFlag)
    t.start()
    threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()
    print("Exiting Main Thread")


if __name__ == "__main__":
    main()
