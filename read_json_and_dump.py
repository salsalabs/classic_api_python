# App to read supporters and dump the full JSON buffer in a useful format.
import argparse
import requests
import yaml
import json

# Get the login credentials
parser = argparse.ArgumentParser(description='Read supporters')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')
parser.add_argument('--pretty', dest='pretty', action='store_true',
                    help='Output pretty JSON. See PyYaml. Default is unformatted.')

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

# Read the HTML content from the email blast victim.
payload = {'json': True,
    'object': 'supporter',
    'condition': 'First_Name=Bob' }
u = 'https://'+ cred['host'] +'/api/getObjects.sjs'
r = s.get(u, params=payload)
j = r.json()
if args.pretty:
    print(yaml.dump(j))
else:
    print(j)
