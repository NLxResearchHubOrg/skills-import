"""
Example Script for OAuth 2.0 Token
"""

import json
import requests

OAUTH2_URL = ''
CLIENT_ID = ''
CLIENT_SECRET = ''
OAUTH2_TOKEN_URL = '{}/oauth/token'.format(OAUTH2_URL)

def get_token():
    headers = {'content-type': 'application/json'}
    data = {'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET,
            'grant_type': 'client_credentials'}
    r = requests.post(OAUTH2_TOKEN_URL, headers=headers, data=json.dumps(data))
    print(r.status_code)
    print(r.json())
    token = r.json()['access_token']

if __name__ == '__main__':
   get_token()
