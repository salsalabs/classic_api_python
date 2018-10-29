# App to save an email blast and confirm the write.
import argparse
import requests
import yaml
import json
import hashlib

# Get the login credentials
parser = argparse.ArgumentParser(description='Write, view and delete an email_blast')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')
parser.add_argument('--html', dest="htmlFile", action='store',
                    help="HTML file to write to a new enail blast")

args = parser.parse_args()
if args.loginFile == None:
    print("Error: --login is REQUIRED.")
    exit(1)
if args.htmlFile == None:
    print("Errror --html is REQUIRED.")
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

# Save this blast content to the database.
f = open(args.htmlFile, 'r')
content = f.read()
f.close()
payload = {
    'json':                 True,
    'object':               'email_blast',
    'HTML_Content':         content,
    'Reference_Name':       'Blast from the API',
    'Subject':              'Blast from the API',
    '_From':                'info@cookie.cutter',
    'From_Name':            'Cookie Cooks, Cookie Cutter Foundation',
    'From Email_address':   'cookie@cookiecutter.cut',
    'Reply_To_Email':	    'yo-cookie-cutter@cookiecutter.cut',
    'Template':             335
}

# Note 1: `/save` must use HTTP POST.  The payload is stored in the body. The
# body is huge (2MB) and it's unlikely that an API call will overrun it.
#
# NOte 2: Results are always returned as a list, even if only one record is
# saved.
u = 'https://' + cred['host'] +'/save'
r = s.post(u, data=payload)
j = r.json()
key = j[0]['key']
print('Saved with key: %s' % key)

# Read to confirm the modification.  The `getObject.sjs` call returns a single
# record as a dictionary.
payload = {
    'json': True,
    'object': 'email_blast',
    'key': key }
u = 'https://' + cred['host'] +'/api/getObject.sjs'
r = s.get(u, params=payload)
j = r.json()

# Write retrieved HTML to "confirmation_[[key]].thml"
# Save this blast content to the database.
fn = "confirmation_%s.html" % (key)
f = open(fn, 'w')
conf_content = j["HTML_Content"]
f.write(conf_content)
f.close()
print("HTML confirmmation can be found in %s" % fn)

before_hash = hashlib.sha256()
before_hash.update(content)
confirm_hash = hashlib.sha256()
confirm_hash.update(conf_content)
print("SHA256 hash in:  %s" % before_hash.hexdigest())
print("SHA256 hash out: %s" % confirm_hash.hexdigest())
