# App to save an email blast and confirm the write.
import argparse
import csv
import json
import requests
import yaml

# Get the login credentials
parser = argparse.ArgumentParser(description='See short district codes')
parser.add_argument('--login', dest='loginFile', action='store', required=True,
                    help='YAML file with login credentials')
parser.add_argument('--csv', dest='csvFile', action='store', default='districts.csv',
                    help="CSV of matching supporter_district records")
parser.add_argument('--offset', dest="offset", action="store", type=int, default='0',
                    help="Start at this offset in the supporter_district table")

args = parser.parse_args()
if args.loginFile == None:
    print("Error: --login is REQUIRED.")
    exit(1)
cred = yaml.load(open(args.loginFile))

# Authenticate
payload = {
    'email':    cred['email'],
    'password': cred['password'],
    'json':     True }
u = 'https://' + cred['host'] + '/api/authenticate.sjs'
s = requests.Session()
r = s.get(u, params=payload)
j = r.json()
if j['status'] == 'error':
    print('Authentication failed: ', j)
    exit(1)

# Read supporter_districts.  Write district info and supporter_KEY
# when the district type is "SS" or "SH" and the district key is
# less than 5 digits.
payload = {
    'json':      True,
    'object':    'supporter_district(supporter_KEY)supporter',
    'condition': 'district_type IN SH,SS&condition=district_code NOT LIKE %00%',
    'include':   'supporter_district.supporter_KEY,district_type,district_code'
}
headers = [ k.replace('supporter_district.', '') for k in payload['include'].split(',') ]
skips = 'key,object,supporter_district_KEY'.split(',')

u = f"https://{cred['host']}/api/getLeftJoin.sjs"
csvFile = None
writer = None
offset = args.offset
count = 500
while count == 500:
    payload['limit'] = f"{offset},{count}"
    if offset % 10e3 == 0:
        print(f"{payload['limit']}")
    r = s.get(u, params=payload)
    try:
        j = r.json()
    except json.decoder.JSONDecodeError as e:
        print(e)
        print(r.text)
        exit(1)
    for row in j:
        # Remove keys that the DictWriter woud find issue with.
        for k in skips:
             if k in row: del row[k]

        if len(row['district_code']) < 5:
            if writer == None:
                csvFile = open(args.csvFile, 'w')
                # DictWriter requires the headers but doesn't write them.
                # We'll do that first...
                w = csv.writer(csvFile)
                w.writerow(headers)
                writer = csv.DictWriter(csvFile, headers)
            writer.writerow(row)
    offset += count
print(f"done {offset}")
writer.flush()
csvFile.close()
