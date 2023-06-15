import pandas as pd
import numpy as np 
import os 
import datetime
import pytz
from tqdm import tqdm



def check_exist(relative_url_upload, file_url):
    flag_check_ex = False
    file_name = file_url.split('/')[-1].split('.')[0]
    df_files_ex = BIHub.get_content_url('/sites/BIHub/Shared Documents/Test')
    ls_files_name_ex = df_files_ex['Name'].str.split('.').str[0].to_list()
    if file_name in ls_files_name_ex:
        flag_check_ex = True
    return flag_check_ex



def find_col_long_lat_geo(dataframe):
    ls_cols=dataframe.columns.tolist()
    long = list(filter(lambda x: 'LONG' in x.upper(), ls_cols))
    lat = list(filter(lambda x: 'LAT' in x.upper(), ls_cols))
    lat_col = ''
    long_col = ''
    if len(lat) > 1:
        for i in lat:
            if len(i.split('.')) == 1:
                if i.split('.')[0].upper() == lat_col.upper():
                    print('Check duplicate columns!')
                else:
                    lat_col = i
            else:
                print('Detecting...')
    else:
        try:
            lat_col = lat[0]
        except:
            print('Can not find latitude column')

    if len(long) > 1:
        for j in long:
            if len(j.split('.')) == 1:
                if j.split('.')[0].upper() == long_col.upper():
                    print('Check duplicate columns!')
                else:
                    long_col = j
            else:
                print('Detecting...')
    else:
        try:
            long_col = long[0]
        except:
            print('Can not find longitude column')

    return lat_col, long_col

def send_mail(content):
    import json
    import msal
    import requests
    client_id = 'd66b4e6d-9bfd-4a8e-b191-be8a64c80176'
    client_secret = '~jh7Q~2WJK6j-bBLURxPWYymvAvhO-69-dfCq'
    tenant_id = 'd929b288-ebb1-49e2-8435-99994c5663d6'
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    html = content.to_html()

    scopes = ["https://graph.microsoft.com/.default"]

    app = msal.ConfidentialClientApplication(
      client_id=client_id,
      client_credential=client_secret,
      authority=authority)

    response = app.acquire_token_for_client(scopes=scopes)
    response = None
    response = app.acquire_token_silent(scopes, account=None)


    if "access_token" in response:
        userId = "D3VNResearch@savills.com.vn"
        endpoint = f'https://graph.microsoft.com/v1.0/users/{userId}/sendMail'
        toUserEmail = "hcmcbi-intern03@savills.com.vn"
        email_content = {'Message': {'Subject': "Test Sending Email from Python",
                               'Body': {'ContentType': 'HTML','Content': '<p>Dear all,<br>Duplicate row: </p><h4>'+html+'</h4>'},
                               'ToRecipients': [{'EmailAddress': {'Address': toUserEmail}}]
                               },
                   'SaveToSentItems': 'true'}
        r = requests.post(endpoint,
                        headers={'Authorization': 'Bearer ' + response['access_token']}, json=email_content)
        if r.ok:
            print('Sent email successfully')
        else:
            print(r.json())
    else:
        print(response.get("error"))
        print(response.get("error_description"))
        print(response.get("correlation_id"))
def Convert_District(data,cnxn):
    Raw_dictrict=pd.read_sql('select * from GENERAL.District_Dictionary',cnxn)
    Raw_dictrict['Raw_District']=Raw_dictrict['Raw_District'].str.strip().str.lower()
    District=dict(zip(Raw_dictrict['Raw_District'],Raw_dictrict['Cleaned_District']))
    data['Project_District_Name']=data['Project_District_Name'].str.strip().str.lower()
    data['Project_District_Name']=data['Project_District_Name'].replace(District)
    data['Project_District_Name']=data['Project_District_Name'].str.title()
    return data
# detect value not in dictionary
def Check_District(data,cnxn):
    flag=0
    Raw_dictrict=pd.read_sql('select * from GENERAL.District_Dictionary',cnxn)
    Raw_dictrict['Raw_District']=Raw_dictrict['Raw_District'].str.strip().str.lower()
    data['Project_District_Name']=data['Project_District_Name'].str.strip().str.lower()
    data['Project_District_Name']=data['Project_District_Name'].apply(lambda x: None if x == '' else x )
    data['Convert_District'] = pd.merge(data, Raw_dictrict, how='left', left_on='Project_District_Name', right_on='Raw_District')['Cleaned_District']
    #
    district_not_in_dict = data['Project_District_Name'][data['Convert_District'].isnull()]
    if len(district_not_in_dict) != 0 and set(district_not_in_dict.tolist()) != {None}:
        flag=1
        return flag,data,district_not_in_dict
    else:
        data['Project_District_Name'] = data['Convert_District']
        data = data.drop(columns=['Convert_District'])
        return flag,data,district_not_in_dict
def Check_Status(data,cnxn):
    flag=0
    Raw_status=pd.read_sql('select * from [GENERAL].[Project_Status_Dictionary]',cnxn)
    Raw_status['Raw_Status']=Raw_status['Raw_Status'].str.strip().str.lower()
    data['Project_Status']=data['Project_Status'].str.strip().str.lower()
    data['Convert_Status'] = pd.merge(data, Raw_status, how='left', left_on='Project_Status', right_on='Raw_Status')['Cleaned_Status']
    #
    status_not_in_dict = data['Project_Status'][data['Convert_Status'].isnull()]
    if len(status_not_in_dict) != 0 and set(status_not_in_dict.tolist()) != {None}:
        flag=1
        return flag,data,status_not_in_dict
    else:
        data['Project_Status'] = data['Convert_Status']
        data = data.drop(columns=['Convert_Status'])
        return flag,data,status_not_in_dict
# detect value not in dictionary
def Check_Type_1(data,cnxn,Sector):
    if Sector=='RETAIL' or Sector=='VLTH' or Sector == 'APARTMENT' :
        Raw_Type=pd.read_sql("select * from GENERAL.Project_Type_Dictionary WHERE Sector = '{}' ".format(Sector),cnxn)
        flag=0
        Raw_Type['Raw_Type']=Raw_Type['Raw_Type'].str.strip().str.lower()
        data['Sub_Project_Type']=data['Sub_Project_Type'].str.strip().str.lower()
        data['Convert_Type'] = pd.merge(data, Raw_Type, how='left', left_on='Sub_Project_Type', right_on='Raw_Type')['Cleaned_Type']
        Type_not_in_dict = data['Sub_Project_Type'][data['Convert_Type'].isnull()]
        if len(Type_not_in_dict) != 0 and set(Type_not_in_dict.tolist()) != {None}:
            flag=1
            return flag,data,Type_not_in_dict
        else:
            data['Sub_Project_Type']= data['Convert_Type']
            data = data.drop(columns=['Convert_Type'])
            return flag,data,Type_not_in_dict
    else:
        return 0,data,0
def Check_Grade(data,cnxn,Sector):
    if Sector == 'HOTEL' or Sector == 'OFFICE' or Sector == 'APARTMENT' or Sector == 'SERVICED_APARTMENT':
        Raw_Grade=pd.read_sql('select * from [GENERAL].[Project_Grade_Dictionary]',cnxn)
        flag=0
        Raw_Grade['Raw_Grade']=Raw_Grade['Raw_Grade'].str.strip().str.lower()
        #Raw_Grade['Raw_Grade']=Raw_Grade['Raw_Grade'].dropna()
        data['Grade']=data['Grade'].str.strip().str.lower()
        data['Convert_Grade'] = pd.merge(data, Raw_Grade, how='left', left_on='Grade', right_on='Raw_Grade')['Cleaned_Grade']
    #
        Type_not_in_dict = data['Grade'][data['Convert_Grade'].isnull()]
        if len(Type_not_in_dict) != 0 and set(Type_not_in_dict.tolist()) != {None}:
            flag=1
            return flag,data,Type_not_in_dict
        else:
            data['Grade'] = data['Convert_Grade']
            data = data.drop(columns=['Convert_Grade'])
            return flag,data,Type_not_in_dict
    else:
        return 0,data,0




    

