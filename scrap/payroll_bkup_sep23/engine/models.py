"""
Module Name: forms.py
Install Date : 07-AUG-2020 (Initial)
Modified Date : []
Functionality : Handles the authorization
Version Notes:
07-AUG-2020     -   Initial Push to Production
"""
import os
import json
from werkzeug.security import check_password_hash

class Authorize:
    """ Links the json authentication file
    Note :Temporarily uses a json file , DB will be used in future"""
    def __init__(self):
        self.config_dir = (os.path.abspath(os.path.join(os.getcwd())))
        self.config_file = '/config/auth.json'

    def verify(self, cid, pwd):
        """ Validates user name and password """
        with open(self.config_dir+self.config_file) as auth_file:
            val = json.load(auth_file)
        db_cid = val['cid']
        db_pwd = val['pwd']
        response = db_cid == cid and  check_password_hash(db_pwd, pwd)
        return response
