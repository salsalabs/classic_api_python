# App to save an email blast and confirm the write.
import argparse
import requests
import yaml
import json

# Get the login credentials
parser = argparse.ArgumentParser(description='Write, view and delete an email_blast')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')
parser.add_argument('--email_blast_KEY', dest="email_blast_key", action="store",
                    help="Save HTML from this blast")

args = parser.parse_args()
if args.loginFile == None:
    print("Error: --login is REQUIRED.")
    exit(1)
if args.email_blast_key == None:
    print("Error: --email_blast_KEY is REQUIRED.")
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

print('Authentication: ', j)


# Read to blast's key.  The `getObject.sjs` call returns a single
# record as a dictionary.
payload = {
    'json': True,
    'object': 'email_blast',
    'key': args.email_blast_key }
u = 'https://' + cred['host'] +'/api/getObject.sjs'
r = s.get(u, params=payload)
j = r.json()

# Write retrieved HTML to "confirmation_[[key]].thml"
# Save this blast content to the database.
fn = "blast_%s.html" % (args.email_blast_key)
f = open(fn, 'w')
content = f.write(j["HTML_Content"])
f.close()
print("HTML content can be found in %s" % fn)
