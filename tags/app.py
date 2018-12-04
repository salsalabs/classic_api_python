'''Application to read all supporters and their associated tags, then
write the data to a CSV file.'''

import argparse
import logging
import requests
import time
import yaml

from lckqueue import LockedQueue
from salsa import Authenticate
from supporter_tags import SupporterTagReader, SupporterTagSaver


def main():
    ''' The app starts a supporter reader and several listeners.  The reader
    retrieves qualified supporters from Salsa. 

    The application starts from the command line.  The user provides a file
    containing Salsa campaign manager credentials in a YAML file. The user may
    also set the directory where CSV files will be written.
    
    python3 splitter/app.py --help
    usage: app.py [-h] [--login LOGINFILE] [--dir OUTPUTDIR]
    
    Export supporters, groups and donations for export to Engage.
    
    optional arguments:
      -h, --help         show this help message and exit
      --login LOGINFILE  YAML file with login credentials
      --dir OUTPUTDIR    Store export files here
      --start OFFSET     Start extraction at this offset
 
    Note:
    
    This application *will not* run with python2.  Use python3.
    '''
    
    exitFlag = False
    supporterSaveQueue = LockedQueue()
    threads = []

    # Get the login credentials
    parser = argparse.ArgumentParser(description='Export supporters and their tags from Salsa Classic.')
    parser.add_argument('--login', dest='loginFile', action='store',
                        help='YAML file with login credentials')
    parser.add_argument('--dir', dest='outputDir', action='store', default='./data',
                        help='Store export files here')
    parser.add_argument('--offset', dest='offset', action='store', type=int, default=0,
                        help='Start extraction at this offset')

    args = parser.parse_args()
    # Session to use for network I/O with automatic cookies.
    session = requests.Session()
    # Get the login credntials from a file.
    cred = yaml.load(open(args.loginFile))
    # Login.  Die if the crednentials are wrong.
    Authenticate(cred, session)

    #Default level is INFO so that Python internals won't log debug messages.
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-18s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='./tags.log',
                        filemode='a')
    console = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    console.setLevel(logging.DEBUG)
    log = logging.getLogger(__name__)
    log.addHandler(console)

    # Common kwargs to reduce startup complexity.
    kwargs = {
        'threadID':       1,
        'cred':           cred,
        'session':        session,
        'supporterSaveQueue':   supporterSaveQueue,
        'exitFlag':       exitFlag,
        'log':            log,
        'offset':         args.offset,
        'outputDir':      args.outputDir
    }
  
    # List of threads to start. This is an ordered list.  The order assures that
    # queues are open for reading before anyone tries to send to it.
    tasks = [
      SupporterTagSaver,
      SupporterTagReader]
    
    for task in tasks:
        t = task(**kwargs)
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()
    log.info('Exiting main')


if __name__ == '__main__':
    main()
