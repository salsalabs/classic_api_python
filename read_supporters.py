# App to read some supporters via the Classic API.
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

# Read the HTML content from the email blast victim.
payload = {'json': True,
    'object': 'supporter',
    'condition': 'First_Name=Bob',
    'include': 'supporter_KEY,First_Name,Last_Name,Email' }
u = 'https://'+ cred['host'] +'/api/getObjects.sjs'
r = s.get(u, params=payload)
j = r.json()
f = '{:10}{:10} {:10} {:20}'

for supporter in j:
    print(f.format(supporter["supporter_KEY"],
        supporter["First_Name"],
        supporter["Last_Name"],
        supporter["Email"]
        ))
