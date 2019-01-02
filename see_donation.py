# App to save an email donation and confirm the write.
import argparse
import requests
import yaml
import json
import json2html
from json2html import *

# Get the login credentials
parser = argparse.ArgumentParser(description='Write, view and delete an donation')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')
parser.add_argument('--donation_KEY', dest="donation_key", action="store",
                    help="Save HTML from this donation")

args = parser.parse_args()
if args.loginFile == None:
    print("Error: --login is REQUIRED.")
    exit(1)
if args.donation_key == None:
    print("Error: --donation_KEY is REQUIRED.")
    exit(1)
cred = yaml.load(open(args.loginFile))

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

# print('Authentication: ', j)

# Read for donation's key.  The `getObject.sjs` call returns a single
# record as a dictionary.
payload = {
    'json': True,
    'object': 'donation',
    'key': args.donation_key }
u = 'https://' + cred['host'] +'/api/getObject.sjs'
r = s.get(u, params=payload)
j = r.json()
print(j)
t = json2html.convert(json=j)
# Write retrieved HTML to "donation_[[key]].thml"
fn = "donation_%s.html" % (args.donation_key)
f = open(fn, 'w')
f.write(t)
f.close()
print("HTML content can be found in %s" % fn)
