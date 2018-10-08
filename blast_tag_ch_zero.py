# App to read read "tags" for supporters that have an "email_blast" tag and
# chapter_KEY of zero.  Use the "table_KEY" value to see if there is a matching
# unsubscribe record.  Write some output.  We'll see if the output indicates
# a pattern.
import argparse
import requests
import yaml
import json
import multidict
import csv 

# Get the login credentials
parser = argparse.ArgumentParser(description='Read supporters')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')
parser.add_argument('--csv', dest='csvFile', action='store', default="tags.csv",
                    help="CSV file to contain the output records")
args = parser.parse_args()
cred = yaml.load(open(args.loginFile))
writer = None

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

print('Authentication: ', j)

conditions = [
    "tag.prefix=email_blast",
    "tag_data.database_table_KEY=142",
    "tag_data.chapter_KEY=0",
]
includes = [
    "tag.prefix",
    "tag.tag",
    "tag_data.table_KEY",
    "tag_data.Last_Modified",
    "supporter_KEY",
    "Unsubscribe_Date"
]
payload = multidict.MultiDict([
    ('json', True),
    ('object', 'unsubscribe(supporter_KEY=tag_data.table_KEY)tag_data(tag_KEY)tag'),
    ('condition', 'tag.prefix=email_blast'),
    ('condition',  'tag_data.database_table_KEY=142'),
    ('condition', 'tag_data.chapter_KEY=0'),
    ('include', ','.join(includes))
])
print(payload)
u = 'https://'+ cred['host'] +'/api/getLeftJoin.sjs'

#Passing MultiDict to requests doesn't seem to work.
#We'll fake it by creating a really long URL because HTTP GET.

f1 = '{}?{}={}'
f2 = '{}&{}={}'
for k,n in payload.items():
    if k == 'json':
        u = f1.format(u, k, n)
    else:
        u = f2.format(u, k, n)
r = s.get(u)

print(r.url)
print(r.status_code)
print(r.text)

# This automatcally converts the JSON buffer to an array of dictionaries.
j = r.json()
if j.status_code != 200:
    print("\nRead error")
    print("Status code:", j.status_code)
    print("Body:", j.text)
    exit(0)

print("Found ", len(j), ' records.')
# Iterate through the hashes and display essential parts of the record.
f = '{:10}:{:10} {:10} {:20} {:20} {:20} {:20}å'
for tag in j:
    if writer == None:
        writer = csv.DictWriter(open(args.csvFile, 'w'), tag.keys())
    print(f.format(tag["prefix"],
     tag["tag"],
     tag["table_KEY"],
     tag["chapter_KEY"],
     tag["Last_Modified"],
     tag["supporter_KEY"],
     tag["Unsubscribe_Data"]))
    writer.writeDict(j)
writer.close()
print("Done")

