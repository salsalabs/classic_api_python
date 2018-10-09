# App to read read the "tags" tables for supporters that have an "email_blast" tag
# and a chapter_KEY of zero.  Use the "table_KEY" value to see if there is a matching
# unsubscribe record.
#
# Write some output as a CSV file. Records marked "interesting" appeara
# to be unsubscribed before the email blast tag was applied.

import argparse
import csv 
import datetime
import json
import multidict
import requests
import yaml

def parse_date(x):
    # Parse Salsa Classic dates into a datetime.
    # Input is like "Mon Oct 09 2017 19:25:56 GMT-0400"
    # Output is a datetime object.
    x = x.split(" GMT")[0].strip()
    f = '%a %b %d %Y %X'
    return datetime.datetime.strptime(x, f)

def handle(j, csvFN, writer):
    # Process the contents of the JSON object.  Creates a CSV writer
    # with filename `csvFN`. Returns the writer.

    print("Found ", len(j), ' records.')
    # Iterate through the hashes and display essential parts of the record.
    f = '{:10}:{:10} {:10} {:20} {:20} {:20} {:5}'
    for r in j:
        if writer == None:
            heads = 'tag_data_KEY,prefix,tag,table_KEY,Last_Modified,Unsubscribe_Date,interesting'.split(",")
            csvFile = open(csvFN, 'w')

            # DictWriter requires the headers but doesn't write them.
            # We'll do that first...
            w = csv.writer(csvFile)
            w.writerow(heads)

            writer = csv.DictWriter(csvFile, heads)

        # If the unsub time is less than the tag's last modified, then
        # that suggests that the chapter_KEY of zero is legitimate.

        t1 = parse_date(r["Unsubscribe_Date"])
        t2 = parse_date(r["Last_Modified"])
        r["interesting"] = t1 < t2

        # Replace the dates with easier-to-read versions.
        r["Unsubscribe_Date"] = '{0:%Y-%m-%d %H:%M:%S}'.format(t1)
        r["Last_Modified"] = '{0:%Y-%m-%d %H:%M:%S}'.format(t2)

        # CSV writer is touchy about keys in the dict that are
        # not in the headers. In this case, it's the primary key
        # for the first table in the join.
        del(r['unsubscribe_KEY'])

        writer.writerow(r)

    return writer

def main():
    # Get the login credentials
    parser = argparse.ArgumentParser(description='Read supporters')
    parser.add_argument('--login', dest='loginFile', action='store',
                        help='YAML file with login credentials')
    parser.add_argument('--offset', dest='offset', type=int, default=0,
                        help="Start searching at this offset")
    parser.add_argument('--csv', dest='csvFN', action='store', default="tags.csv",
                        help="CSV file to contain the output records")
    args = parser.parse_args()
    cred = yaml.load(open(args.loginFile))
    writer = None
    csvFile = None

    # Authenticate
    payload = {
        'email': cred['email'],
        'password': cred['password'],
        'json': True }
    s = requests.Session()
    u = 'https://' + cred['host'] + '/api/authenticate.sjs'
    r = s.get(u, params=payload)
    j = r.json()
    if j['status'] == 'error':
        print('Authentication failed: ', j)
        exit(1)

    print('Authentication: ', j['status'])

    conditions = [
        "tag.prefix=email_blast",
        "tag_data.database_table_KEY=142",
        "tag_data.chapter_KEY=0"
    ]

    includes = [
        "tag_data.tag_data_KEY",
        "tag.prefix",
        "tag.tag",
        "tag_data.table_KEY",
        "tag_data.Last_Modified",
        "Unsubscribe_Date"
    ]

    # We're using MultiDict because it is is more expressive
    # than a list of things. Also easier to relate back to 
    # the API doc.
    payload = {
        'json': True,
        'object': 'unsubscribe(supporter_KEY=tag_data.table_KEY)tag_data(tag_KEY)tag',
        'condition': conditions,
        'include': ','.join(includes),
        'count': 500,
        'offset': args.offset
         }

    # But... passing MultiDict to requests doesn't seem to work.
    # We'll fake it by creating a really long URL because HTTP GET.

    while payload['count'] > 0:
        f = 'Reading {} from offset {}'
        print(f.format(payload['count'], payload['offset']))
        u = build_url(cred, payload)

        r = s.get(u, params=payload)
        if r.status_code != 200:
            print("\nRead error")
            print("Status code:", j.status_code)
            print("Body:", j.text)
            print()
            exit(0)

        # This automatcally converts the JSON buffer to an array of dictionaries.
        j = r.json()
        # Interate through the dictionaries.
        if len(j) > 0:
            writer = handle(j, args.csvFN, writer)
        payload['count'] = len(j)
        payload['offset'] = payload['offset'] + payload['count']
    print("Done")

main()