from simple_salesforce import Salesforce
from retrying import retry
import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

load_dotenv()


def get_sf_access_token(sf_token_url,sf_client_id,sf_client_secret):
        payload = {
            'grant_type': 'client_credentials',
            'client_id': sf_client_id,
            'client_secret': sf_client_secret,
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        response = requests.post(sf_token_url, data=payload, headers=headers, allow_redirects=False)
        while response.status_code in (301, 302, 303, 307, 308):
            response = requests.post(response.headers['Location'], data=payload, headers=headers, allow_redirects=False)

        if response.status_code == 200:
            token_data = response.json()
            return token_data.get('access_token'), token_data.get('instance_url')
        else:
            print(f"Auth Error: {response.status_code} - {response.text}")
            return None, None

def get_sf_instance(sf_token_url,sf_client_id,sf_client_secret): 
    session_id, instance_url = get_sf_access_token(sf_token_url, sf_client_id, sf_client_secret)
    return Salesforce(instance_url=instance_url, session_id=session_id)


