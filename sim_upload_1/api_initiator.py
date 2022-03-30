import requests
from config import sim_file_upload_api_config, validate_api_user, sim_part_price_api_config, sim_inventory_api_config
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class api_call:
    def __init__(self):
        self.token = self.auth_user()

    def auth_user(self):
        """
        Method to generate auth token
        :return: Auth token
        """
        token = ''
        data = {
            "username" : validate_api_user["username"],
            "password" : validate_api_user["password"]
        }
        print(data["username"])
        header = {'Content-Type': 'application/json'}
        print(validate_api_user['validate_api_user_url'])

        try :

          response = requests.request("POST", validate_api_user['validate_api_user_url'], data = data, verify=False)
        except Exception as e:
            print(e)

        if response.status_code == 200:
            auth_resp = json.loads(response.content)
            token = auth_resp["token"]

        return token

    def sim_csv_upload(self, csv_path , url, token):
        """
        Method to call sim state change API
        :param data: API payload data
        :return: API Response code and payload
        """
        payload = {}
        files = [

            ('fileInput', ('file', open(csv_path, 'rb'), 'application/octet-stream'))

        ]

        header = {'Authorization': token}
        status_code = 401
        retry = 2
        while status_code == 401 and retry:
            #print(retry, url, header)
            response = requests.request("POST", url, headers=header, files=files, verify = False)
            #response = requests.request("POST", url, headers=header, data=payload, files=files, verify=False)
            status_code = response.status_code
            content = json.loads(response.content)
            retry = retry - 1
            if status_code == 401 and retry:
                header["Authorization"] = self.token = self.auth_user()
            #print(response.json)
        return status_code, content


    def sim_part_price(self, data, token):
        """
        Method to call sim state change API
        :param data: API payload data
        :return: API Response code and payload
        """

        url = sim_part_price_api_config["sim_part_price"]
        header = {'Content-Type': 'application/json',
                  'Authorization': token}
        status_code = 401
        retry = 2
        while status_code == 401 and retry:
            print(retry, url, header, data)
            response = requests.request("PUT", url, data = data, verify=False)
            status_code = response.status_code
            content = json.loads(response.content)
            retry = retry - 1
            if status_code == 401 and retry:
                header["Authorization"] = self.token = self.auth_user()
        return status_code, content

    def sim_inventory(self, data, token):
        """
        Method to call sim state change API
        :param data: API payload data
        :return: API Response code and payload
        """

        url = sim_inventory_api_config["sim_inventory"]
        header = {'Content-Type': 'application/json',
                  'Authorization': token}
        status_code = 401
        retry = 2
        while status_code == 401 and retry:
            print(retry, url, header, data)
            response = requests.request("PUT", url, data=data, verify=False)
            status_code = response.status_code
            content = json.loads(response.content)
            retry = retry - 1
            if status_code == 401 and retry:
                header["Authorization"] = self.token = self.auth_user()
        return status_code, content
