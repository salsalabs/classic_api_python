# App to get support_action records with specific action keys.
import argparse
import requests
import yaml
import json

# Get the login credentials
parser = argparse.ArgumentParser(description='Read supporters')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')
parser.add_argument('--input', dest='keyFile', action='store',
		   help='Backup file of supporter_action_KEY records to delete')

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

f = open(args.keyFile, 'r')
keys = [ line.split('\t')[0] for line in f ]
f.close()

# We have a backup in the file.  That means that we can just whack
# the records without the API examining them first.
for key in keys:
    print(f"{key}")
    payload = {'json': True,
        'object': 'supporter_action',
        'key': key }
    u = 'https://'+ cred['host'] +'/delete'
    r = s.get(u, params=payload)
    print(f"{key}: {r.json()}")
