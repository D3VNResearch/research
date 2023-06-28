from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.client_request_exception import ClientRequestException
import datetime
import pytz
import pandas as pd
import numpy as np
import csv
import glob
import pyodbc
import os 
import json
from io import BytesIO
import io
import platform
from function.PyToSp import *
from tqdm import tqdm
from urllib.parse import quote_plus
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, event,text
import pyodbc
import requests
import inspect
from validate import *
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
from urllib.parse import quote_plus
import msal
from itertools import chain
from send_email import *
import json



#Connnect SQL Server 
def ConnectAzureSQLServer(): 
    f = open ('database_information.json', "r") #Database information file, can change information depending on the time
    qq = json.loads(f.read())
    f.close()
    ini_cnt_str ='Driver={driver_str};Server=tcp:{servername},1433;database={database};Uid={username};Pwd={password};Encrypt=yes;Authentication=ActiveDirectoryPassword;Connection Timeout=30;'.format(**qq)
    quoted = quote_plus(ini_cnt_str)
    cnt_str = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(cnt_str)

        #Test Connection
    try:
        conn = engine.connect()
        result = conn.execute(text("SELECT 1"))
        print("CONNECTION SUCESSFUL!")
    except Exception as e:
        print("CONNECTION FAILED:",str(e))
    return cnt_str
# Connect SharePoint Get FlatFile
#url_hub= input(str) Khi xay dung he thong co nhieu hub thi thay doi kieu input
def ConnectSharePoint(url_hub):
    header_Hub = f"share_point_{url_hub.split('/')[2].replace('-','')}"
    config_Hub = read_config_json(config_path, header_Hub)
    Hub = SharePoint(config_Hub)
    #Hub.check_connect()
    return Hub
