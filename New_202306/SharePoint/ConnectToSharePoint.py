from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.client_request_exception import ClientRequestException
import pandas as pd
import os 
import json
import datetime
import io
import platform
import chardet
from io import StringIO, BytesIO


ROOT_PATH = os.getcwd()
config_path = os.path.join(ROOT_PATH, 'config.json')


# read json config file
def read_config_json(path, header):
    with open(path) as config_file:
        config = json.load(config_file)
        config = config[header]
    return config

class SharePoint:
    def __init__(self, config):
        self.context_auth = AuthenticationContext(config['url'])
        self.context_auth.acquire_token_for_app(client_id=config['client_id'], 
                                           client_secret=config['client_secret'])
        self.ctx = ClientContext(config['url'], self.context_auth)

    def check_connect(self):
        self.web = self.ctx.web
        self.ctx.load(self.web)
        self.ctx.execute_query()
        print("Web site title: {0} _ Successful Connection!".format(self.web.properties['Title']))




def main():
    header_BIHub = 'share_point_BIHub'
    config_BIHub = read_config_json(config_path, header_BIHub)
    BIHub = SharePoint(config_BIHub)
    BIHub.check_connect()


if __name__ == "__main__":
    main()