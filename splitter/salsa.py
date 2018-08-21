def Authenticate(cred, s):
    # Authenticate with Salsa.  Errors are fatal.
    payload = {
        'email': cred['email'],
        'password': cred['password'],
        'json': True}
    u = 'https://' + cred['host'] + '/api/authenticate.sjs'
    r = s.get(u, params=payload)
    j = r.json()
    if j['status'] == 'error' or j["message"] != "Successful Login":
        print('Authentication failed: ', j)
        exit(1)
