# App to get support_action records with specific action keys.
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



# Get the description of the supporter_action table
# payload = {'json': True,
#     'object': 'supporter_action' }
# u = 'https://'+ cred['host'] +'/api/describe2.sjs'
# r = s.get(u, params=payload)
# print('My print statement: ', r.text)
# j = r.json()
# exit(0)



# Read the HTML content from the supporter_action victim.
payload = {'json': True,
    'object': 'supporter_action',
    'condition': 'action_KEY IN 15942,15943,18140,23426',
    'include': 'supporter_action_KEY,organization_KEY,supporter_KEY,action_KEY' }
u = 'https://'+ cred['host'] +'/api/getObjects.sjs'
r = s.get(u, params=payload)
# print('My print statement: ', r.text)
# exit(0)
j = r.json()
f = '{:10} {:10} {:10} {:10}'

for supporter_action in j:
    print(f.format(supporter_action["supporter_action_KEY"],
        supporter_action["organization_KEY"],
        supporter_action["supporter_KEY"],
        supporter_action["action_KEY"]
        ))
    payload = {'json': True,
        'object': 'supporter_action',
        'condition': 'supporter_action_KEY=supporter_action["supporter_action_KEY"]' }
    u = 'https://'+ cred['host'] +'/delete'
    r = s.get(u, params=payload)
    print('Deleted ', supporter_action["supporter_action_KEY"], ' XXX ', r.text)

payload = {'json': True,
    'object': 'supporter_action',
    'condition': 'action_KEY IN 15942,15943,18140,23426',
    'include': 'supporter_action_KEY,organization_KEY,supporter_KEY,action_KEY' }
u = 'https://'+ cred['host'] +'/api/getObjects.sjs'
r = s.get(u, params=payload)
# print('My print statement: ', r.text)
# exit(0)
j = r.json()
f = '{:10} {:10} {:10} {:10}'

for supporter_action in j:
    print(f.format(supporter_action["supporter_action_KEY"],
        supporter_action["organization_KEY"],
        supporter_action["supporter_KEY"],
        supporter_action["action_KEY"]
        ))
