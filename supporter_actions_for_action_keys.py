# App to read supporters and dump the full JSON buffer in a useful format.
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

# Read the HTML content from the email blast victim.
payload = {
    'object': 'supporter_action',
    'condition': 'action_KEY IN 15942,15943,18140,23426',
    'include': 'supporter_action_KEY,organization_KEY,supporter_KEY,action_KEY',
    'json': True }
u = 'https://'+ cred['host'] +'/api/getObjects.sjs'
print(u)
r = s.get(u, params=payload)
j = r.json()
print(json.dumps(j, indent=4, sort_keys=True))

f = '{:10}{:10} {:10} {:10}'
for supporter_action in j:
    print(f.format(supporter_action["supporter_action_KEY"],
        supporter_action["organization_KEY"],
        supporter_action["supporter_KEY"],
        supporter_action["action_KEY"]))

# Do you want to traverse the XML?  Yes?
# Cool! Click here https://www.geeksforgeeks.org/xml-parsing-python/
