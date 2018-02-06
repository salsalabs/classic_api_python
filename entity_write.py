# App to modify the HTML content of an email blast with something
# that contains less-thans and greater-thans.

import requests
s = requests.Session()

# Constants.
email = 'aleonard@basledi.com'
password = 'extra-super-secret-password'
host = 'https://wfc2.wiredforchange.com'
content = '<p><? var x = 1; if ( x > 1 && x < 2) { x = 3; } ?>'
table = 'email_blast'
key = 54898

# Authenticate
payload = { 'email': email, 'password': password, 'json': True }
u = host + '/api/authenticate.sjs'
r = s.get(u, params=payload)
j = r.json()
if j['status'] == 'error':
    print("Authentication failed: ", j)
    exit(1)

print("Authentication: ", j)

# Read the HTML content from the email blast victim.
payload = {'json': True, 'object': table, 'key': key }
u = host + '/api/getObject.sjs'
r = s.get(u, params=payload)
j = r.json()
print("Read blast: HTML content is ", j['HTML_Content'])

# Modify and save the HTML content.
payload['HTML_Content'] = j['HTML_Content'] + content
u = host + '/save'
r = s.post(u, data=payload)
print("Save: ", r.json())

# Read to confirm the modification.
payload = {'json': True, 'object': table, 'key': key }
u = host + '/api/getObject.sjs'
r = s.get(u, params=payload)
j = r.json()
print("Confirmation: HTML content is ", j['HTML_Content'])
