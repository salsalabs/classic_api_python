# App to save a supporter and confirm the write.
import argparse
import requests
import yaml
import json

# Get the login credentials
parser = argparse.ArgumentParser(description='Write, view and delete a supporter')
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

payload = {
    'json': True,
    'object': 'supporter',
    'First_Name': 'Bettina',
    'Last_Name': 'Burger',
    'Email': 'bettina@burger.yum'
}
u = 'https://' + cred['host'] +'/save'
r = s.post(u, data=payload)
j = r.json()
print('Save: ', r.json())
key = j[0]["key"]
print("Saved with key: ", key)

# Read to confirm the modification.
payload = {
    'json': True,
    'object': 'supporter',
    'key': key }
u = 'https://' + cred['host'] +'/api/getObject.sjs'
r = s.get(u, params=payload)
supporter = r.json()
print('Insert confirmation: content is ')

f = '{:10}{:10} {:10} {:20}'
print(f.format(supporter["supporter_KEY"],
    supporter["First_Name"],
    supporter["Last_Name"],
    supporter["Email"]
    ))
