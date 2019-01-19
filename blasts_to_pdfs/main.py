# App to save a supporter and confirm the write.
import argparse
import requests
import yaml
import json
import pdfkit
import os.path
import os

# Returns a blast name given a blast record
def get_blast_name(blast):
    p = blast['Date_Created'].split(' ')
    return f"{p[3]}/{p[3]}-{p[2]}-{p[1]} {blast['email_blast_KEY']} {blast['Subject']}.pdf"

# Main does the work
def main():
    # Get the login credentials
    parser = argparse.ArgumentParser(description="Create PDFs for an organization's blasts")
    parser.add_argument('--login', dest='loginFile', action='store',
                        help='YAML file with login credentials')

    args = parser.parse_args()
    if args.loginFile == None:
        print("Error: --login is REQUIRED.")
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
        print('Authentication failed: ', j['message'])
        exit(1)

    # Read blast keys, format URLs and create PDFs.
    offset = 0
    count = 500
    conditions = [
        "Stage=Complete"
    ]
    tables = "email_blast"
    includes = "organization_KEY,email_blast_KEY,Date_Created,Subject"
    while count == 500:
        payload = {'json': True,
                   "limit": f"{offset},{count}",
                   'object': tables,
                   'condition': "&condition=".join(conditions),
                   'include': includes}
        u = f"https://{cred['host']}/api/getObjects.sjs"
        print(f"reading {count} from {offset:7d}")
        r = s.get(u, params=payload)
        j = r.json()
        for blast in j:
            print(blast)
            u = f"https://{cred['host']}/o/{blast['organization_KEY']}/t/0/blastContent.jsp?email_blast_KEY={blast['email_blast_KEY']}"
            print(u)
            f = get_blast_name(blast)
            p = os.path.dirname(f)
            if not os.path.exists(p):
                os.makedirs(p)
            pdfkit.from_url(u, f)

        count = len(j)
        offset += count

main()
