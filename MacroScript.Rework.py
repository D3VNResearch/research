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
# set to_email
to_email = ['nthieu@savills.com.vn','pthihuongnguyen@savills.com.vn', 'hcmcbi-intern04@savills.com .vn']


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
 # Connect SharePoint Get FlatFile
#url_hub= input(str) Khi xay dung he thong co nhieu hub thi thay doi kieu input
url_hub='/sites/BIHub/Shared Documents/Advisory Data/Macro economic/Flat file'
def ConnectSharePoint(url_hub):
    header_Hub = f"share_point_{url_hub.split('/')[2].replace('-','')}"
    config_Hub = read_config_json(config_path, header_Hub)
    Hub = SharePoint(config_Hub)
    #Hub.check_connect()
    return Hub


selected_provinces = []


list_folder = ConnectSharePoint(url_hub).get_content_url(url_hub, return_list_folder=True) 
def GetProvince(input_string):  
    input_list = input_string.split(',')
    province_list = []
    for i in list_folder:
        province_list.append(i.split('/')[7])
    for input_province in input_list:
        input_province = input_province.strip().lower()
        if input_province == "-1":
            break

        matched_provinces = [province for province in province_list if input_province in province.lower()]
        if matched_provinces:
            selected_provinces.extend(matched_provinces)
        else:
            print(f"Không tìm thấy tỉnh chứa từ khóa '{input_province}'.")
    return selected_provinces 

# Input provinces

input_string = input("Nhập tên các tỉnh bạn muốn chọn, cách nhau bằng dấu phẩy (nhập -1 để thoát): ")
# Input Năm và Quý
quarter_year = input("Nhập chuỗi năm và quý (ví dụ: 2023Q1): ")
Year, Quarter = quarter_year[:4], quarter_year[4:]

# In danh sách tỉnh đã chọn, năm và quý
print("Các tỉnh đã chọn:")
for province in GetProvince(input_string):
    print(colored(province,'yellow'))
print(colored("Year:{}, Quarter:{}".format(Year,Quarter),'yellow'))

def findFolder():
    # Tìm các folder chứa từ khóa
    selected_province_folders = [folder for folder in list_folder if any(re.search(keyword, folder, re.IGNORECASE) for keyword in selected_provinces)]
    selected_year_quarter = []
    for folder in selected_province_folders:
        folder_sub2 = folder + '/' + Year + '/' + Quarter
        selected_year_quarter.append(folder_sub2)
    # In danh sách các folder đã tìm thấy
    print("Folder các tỉnh cần import:")
    for folder in selected_province_folders:
        print(colored(folder,'yellow'))

    #Lấy file trong Năm và Quý
    df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
    for i in selected_year_quarter:
        if i.split('/')[7] in selected_provinces and i.split('/')[8] in Year and i.split('/')[9] in Quarter:
            df_summ_file = pd.concat([df_summ_file, ConnectSharePoint(url_hub).get_content_url(i)])
    list_url = df_summ_file['ServerRelativeUrl'].to_list()

#History file
df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
df_summ_file = df_summ_file.reset_index(drop=True)

