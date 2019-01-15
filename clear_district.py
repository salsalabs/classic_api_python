# App to save a supporter and confirm the write.
import argparse
import requests
import yaml
import json

# Retrieve the supporter with the provided email
def get_supporter(cred, session, email):
    # Retrieve a supporter with the provided email address.
    payload = {
        'json': True,
        'object': 'supporter',
        'condition': f"Email={email}"
    }
    u = f"https://{cred['host']}/api/getObjects.sjs"
    r = session.get(u, params=payload)
    j = r.json()
    return j[0]

# See the contents of a supporter record
def see_supporter(supporter):
    f = '{:10}{:10} {:10} {:20} district "{:10}"'
    print(f.format(supporter["supporter_KEY"],
        supporter["First_Name"],
        supporter["Last_Name"],
        supporter["Email"],
        supporter["District"]
        ))

# Main does the work
def main():
    # Get the login credentials
    parser = argparse.ArgumentParser(description='Clear the district field for a supporter')
    parser.add_argument('--login', dest='loginFile', action='store',
                        help='YAML file with login credentials')
    parser.add_argument('--email', dest="email", action="store",
                        help="Email address of supporter to modify")

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
        print('Authentication failed: ', j['message'])
        exit(1)

    print('Authentication: ', j)

    print("Before:")
    supporter = get_supporter(cred, s, args.email)
    see_supporter(supporter)

    # Setting District to '' changes the District field to an empty string.
    # Setting District to None has no effect.
    payload = {
        'json': True,
        'object': 'supporter',
        'key': supporter["supporter_KEY"],
        'District': '' }
    u = 'https://' + cred['host'] +'/save'
    r = s.post(u, data=payload)
    print("/save response", r)

    print("After:")
    supporter = get_supporter(cred, s, args.email)
    see_supporter(supporter)

main()
