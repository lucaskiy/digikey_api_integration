# -*- coding: utf-8 -*-

import requests
import json
import os


class ApiConn:
    URL = 'https://api.digikey.com/v1/oauth2/token'

    def __init__(self):
        self.token = self.api_auth()

    def api_auth(self):
        filename = "../conn/credentials.json"
        with open(filename, 'r') as f:
            credentials = json.load(f)

        try:
            res = requests.post(url=self.URL, data=credentials)

        except Exception as e:
            print(f"Error reaching Odoo API service for authentication. Error -> {e}")
            return False

        if res.status_code == 200:
            print(f"API authentication successful")
            res_json = json.loads(res.text)
            bearer_token = f"Bearer {res_json['access_token']}"

            del credentials["refresh_token"]
            credentials["refresh_token"] = res_json["refresh_token"]
            os.remove(filename)

            with open(filename, 'w') as f:
                json.dump(credentials, f, indent=2)

            return bearer_token

        else:
            print(f"API authentication failed. Code: {res.status_code} -> {str(res.content)}")
