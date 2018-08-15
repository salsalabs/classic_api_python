# App to read some supporters via the Classic API.  API output is a JSON buffer.
# The `requests` package automatically converts that to a dictionary.  The output uses
# the dictionary to display a few fields from each record found.
import argparse
import requests
import yaml
import json

# Get the login credentials
parser = argparse.ArgumentParser(description='Read supporters')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')

args = parser.parse_args()
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

print('Authentication: ', j)

# Read the content for supporters that match the `condition`.  That generally
# results in at least a few records.  Change the name if there aren't any Bobs
# in your supporter list.
payload = {'json': True,
    'object': 'supporter',
    'condition': 'First_Name=Bob',
    'include': 'supporter_KEY,First_Name,Last_Name,Email' }
u = 'https://'+ cred['host'] +'/api/getObjects.sjs'
r = s.get(u, params=payload)

# This automatcally converts the JSON buffer to an array of dictionaries.
j = r.json()

# Iterate through the hashes and display essential parts of the record.
f = '{:10}{:10} {:10} {:20}'
for supporter in j:
    print(f.format(supporter["supporter_KEY"],
        supporter["First_Name"],
        supporter["Last_Name"],
        supporter["Email"]
        ))
