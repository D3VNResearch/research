import pandas as pd
import numpy as np 
import os 
import datetime
import pytz
from tqdm import tqdm
import re
import unicodedata
from urllib.parse import quote_plus
from sqlalchemy import create_engine, event
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
import pyodbc
import requests
import inspect
from validate import *
from unidecode import unidecode
from termcolor import colored

#get data

def get_data(relative_url, file_url):
    header_BIHub = 'share_point_BIHub'
    config_BIHub = read_config_json(config_path, header_BIHub)
    BIHub = SharePoint(config_BIHub)

    sp_object = relative_url.split('/')[2].replace('-', '')
    data = eval(sp_object).get_file(file_url)
    # Convert data to DataFrame if it's not already
    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

    # Remove irrelevant data
    data.dropna(axis=0, how='all', inplace=True)  # remove blank rows
    data.drop(data.columns[data.columns.str.contains('Unnamed')], axis=1, inplace=True)  # remove blank columns
    data.columns = data.columns.str.strip().str.replace(r'\s+', '', regex=True)  # strip column
    file_name = file_url.split('/')[-1].split('.')[0]
    sector = file_url.split('/')[-1].split('_')[0].upper()

    if sector == 'PM':
        sector = file_url.split('/')[-1].split('_')[1].upper()

    if sector == 'APT':
        sector = 'APARTMENT'
    elif sector == 'SA':
        sector = 'SERVICED_APARTMENT'

    return data, file_name, sector

#check date_key
def check_date_key(file_url, data):
    if 'Date_key' in data.columns:
        data = data.rename(columns={'Date_key':'Date_Key'})
    else:
        pass
    sector = file_url.split('/')[-1].split('_')[0].upper()
    file_name = file_url.split('/')[-1].split('.')[0]
    url_key = file_url.split('/')[-1].split('.')[0].split('_')[-1]
    url_key = int(url_key)
    data_date_key = data.groupby(['Date_Key'], dropna = False, as_index=False).first()
    if len(data_date_key) != 1:
        print(colored(f"More than two 'Date_Key' in {file_name}",'yellow'))
        data['Date_Key'] = [url_key for i in data['Date_Key']]     
        return data
    else:
        for i in data['Date_Key']:
            if i != url_key:
                print(colored(f"Different 'Date_Key' between {file_name} and value inside",'yellow'))
                data['Date_Key'] = [url_key for i in data['Date_Key']]
                return data
            else:
                return data

            
def check_duplicate(data, column_name):
    df_dup = data[data.duplicated(subset=[f'{column_name}'], keep='last')]
    return data, df_dup

"""def remove_unformated_character(data):
    #re.sub(pattern, repl, string, count=0, flags=0)
    list_unformated_character = ['#N/a', 'N/A', 'N/a', 'n/a', 'NA', '-'
                                 , '$-', '#DIV/0!', '0', 0, 'FALSE'   
                                ]
    list_keys_on = ['Sector', 'Sub_Project_Name', 'Project_District_Name', 'Project_City_Name'
                    , 'Project_Type', 'Project_Grade'
                    , 'District', 'City', 'Project_Sub_Type'
                   ]
    for columns in data.columns:
        data[columns] = data[columns].replace(r'^\s*$', np.nan, regex=True)#Replacing blank values (has white space)
        data[columns] = data[columns].replace('(^\s+|\s+$)', '', regex=True)
        data[columns] = data[columns].replace(r'\s+', ' ', regex=True)#remove line breaks from an excel cells
        data[columns] = data[columns].replace(r'\.0$','', regex=True)
        if type(columns) == str:
            data[columns] = convert_str(data[columns])
            if columns not in list_keys_on:
                data[columns] = data[columns].replace(',','', regex=True)
            data.loc[:,columns] = data[columns].apply(lambda x: x.title() if isinstance(x, str) else x)
    for i in list_unformated_character:
        data.replace(i, np.nan, inplace=True)
    data = data.replace({np.nan: None})
    return  data"""

def remove_unformated_character(data):
    #re.sub(pattern, repl, string, count=0, flags=0)
    list_unformated_character = ['#N/a', 'N/A', 'N/a', 'n/a', 'NA', '-'
                                 , '$-', '#DIV/0!', '0', 0, 'FALSE' 
                                 , 'Null'
                                ]
    for columns in data.columns:
        data[columns] = data[columns].replace(r'^\s*$', np.nan, regex=True)#Replacing blank values (has white space)
        data[columns] = data.loc[:,columns].fillna(value='NULL')
        data[columns] = data[columns].replace('(^\s+|\s+$)', '', regex=True)
        data[columns] = data[columns].replace(r'\s+', ' ', regex=True)#remove white space in cell + line breaks from an excel cells
        data.loc[:,columns] = data[columns].apply(lambda x: x.title() if isinstance(x, str) else x)
        data[columns] = [re.sub('\.0$', '', str(i)) if type(i) == float else i for i in data[columns]]#remove trailing .0
        #data.loc[:,columns] = data[columns].apply(lambda x: x.title() if isinstance(x, str) else x)
    for i in list_unformated_character:
        data.replace(i, np.nan, inplace=True)
    data = data.replace({np.nan: None})
    return  data
    
#Decode mỗi lần chỉ đổi được 1 Series
def deunicode_funtion(s):
    if type(s)==str:
        s=unidecode(s).strip()
    else:
        s=s
    return str(s)

def remove_unicode(data):
    data = data.fillna('NULL')
    data = data.apply(lambda x:deunicode_funtion(x) if x!=None else x)
    data = data.replace('NULL',np.nan)
    data = data.replace({np.nan: None})
    return data.replace(r'\s+', ' ', regex=True)#remove line breaks from an excel cells
    
def check_dictionary(df_dict, file_name, data, column_name, parameter, sector, cnxn, sp_object):
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x
    if parameter in ['City', 'District', 'Status', 'Indicator'
                     , 'Country', 'Industry'
                    ]:
        raw_parameter = pd.read_sql(f'select * from GENERAL.{parameter}_Dictionary',cnxn)
        raw_parameter[f'Raw_{parameter}'] = raw_parameter[f'Raw_{parameter}'].apply(lower_function)
    elif parameter == 'Grade':
        if sector in ['HOTEL', 'OFFICE', 'APARTMENT'
                      , 'APT', 'SA', 'SERVICED_APARTMENT'
                     ]:
            raw_parameter = pd.read_sql(f"select * from GENERAL.Project_{parameter}_Dictionary",cnxn)
            raw_parameter[f'Raw_{parameter}'] = raw_parameter[f'Raw_{parameter}'].apply(lower_function)
        else:
            return data, df_dict
    elif parameter in ['Type', 'Sub_Type']:
        if sector in ['RETAIL', 'VLTH', 'APARTMENT'
                      , 'APT', 'IP', 'TENANT'
                     ]:
            raw_parameter = pd.read_sql(f"select * from GENERAL.Project_{parameter}_Dictionary WHERE Sector = '{sector}' ",cnxn)
            raw_parameter[f'Raw_{parameter}'] = raw_parameter[f'Raw_{parameter}'].apply(lower_function)
        else:
            return data, df_dict
    elif parameter in ['Project_Name', 'Investor_Nationality', 'Industry_Lv1', 'Investment_Form'
                      , 'Infrastructure_Level_1', 'Infrastructure_Level_2'
                      ]:
        if sector in ['FDI', 'INFRA']:
            header_BIHub = 'share_point_BIHub'
            config_BIHub = read_config_json(config_path, header_BIHub)
            BIHub = SharePoint(config_BIHub)
            raw_parameter = eval(sp_object).get_file(f'/sites/BIHub/Shared Documents/Advisory Data/{sector}/Mapping/{parameter}_Dictionary.xlsx')
            raw_parameter[f'Raw_{parameter}'] = remove_unicode(raw_parameter[f'Raw_{parameter}']).apply(lower_function)
            raw_parameter = raw_parameter.drop_duplicates(subset=[f'Raw_{parameter}'], keep='last')
        else:
            return data, df_dict     
    else:
        print(colored('Unknown parameter in check dictionary section','yellow'))
        return data, df_dict 
    
    data[f'{column_name}']= data[f'{column_name}'].apply(lower_function)
    # data[f'Convert_{parameter}'] = pd.merge(data, raw_parameter
    #                                         , how='left'
    #                                         , left_on=f'{column_name}'
    #                                         , right_on=f'Raw_{parameter}')[f'Cleaned_{parameter}']

    # Hieu update - 31-05-2023
    #get_cleaned_type = lambda x: raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x,f'Cleaned_{parameter}'].values[0] #fix lỗi duplicate của hàm merge trên\
    get_cleaned_type = lambda x: raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x, f'Cleaned_{parameter}'].values[0] if len(raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x, f'Cleaned_{parameter}']) > 0 else None
    print(get_cleaned_type)
    data[f'Convert_{parameter}'] = data[f'{column_name}'].map(get_cleaned_type)
    ## End - Hieu update - 31-05-2023

    parameter_not_in_dict = data[f'{column_name}'][data[f'Convert_{parameter}'].isnull()]
    
    if len(parameter_not_in_dict) != 0:
        temp_df = pd.DataFrame()
        parameter_not_in_dict = list(set(parameter_not_in_dict))
        temp_df.insert(loc=0, column = 'Missing_Values', value = parameter_not_in_dict)
        temp_df.insert(loc=0, column = 'File_Name', value = f'{file_name}')
        temp_df.insert(loc=0, column = 'Flag', value = f'{parameter}')
        df_dict = pd.concat([df_dict, temp_df], axis=0)
        return data, df_dict
    else:
        if parameter == 'Project_Name':
            data[f'{column_name}'] = data[f'{column_name}'].str.title()
            data[f'Convert_{parameter}'] = data[f'Convert_{parameter}'].str.title()
            data.rename(columns = {f'Convert_{parameter}':f'{parameter}_Eng'}, inplace = True)
        else:
            data[f'{column_name}'] = data[f'Convert_{parameter}']
            data = data.drop(columns = [f'Convert_{parameter}'])
        return data, df_dict
    

def convert_float(x):
    if x!=None:
        x=str(x).replace(',','')#has comma as format of excel
    try:
        return float(x)
    except:
        return None#return none as datatype set up in DB
    
    
def convert_int(x):
    try:
        return int(x)
    except:
        return None
    
    
def convert_str(df):
    df = df.fillna('NULL')
    #df = df.astype(str)
    df = df.apply(lambda x: str(x) if isinstance(x, str) else x)
    df = df.replace('NULL',np.nan)
    df = df.replace({np.nan: None})
    return df.replace('\.0$','', regex=True)


def Generate_Additional_Columns(data,df_summ_file,BIHub,engine,file_url):
        #Generate file Date
    project_name=file_url.split('/')[-1]
    history=df_summ_file[df_summ_file.Name==project_name]
    File_Date=history.iloc[0,3]
    data['File_Date']=File_Date
    #Generate Import Date
    today=pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    data['Import_Date']=today
    #Generate Project Name
    project_name=file_url.split('/')[-1]
    project_name=project_name.split('.')[0]
    data['File_Name']=project_name 
    return data

#-------------------------------------------------------
###Create tracking dataframe for flat file
def merge(*iters):#concat multi list
    for i in iters:
        yield from i
"""def tracking_flat_file(data, sector):
    if sector in ['RETAIL', 'OFFICE', 'HOTEL', 'SA', 'SERVICED_APARTMENT', 'APARTMENT', 'APT', 'VLTH']:
        list_columns_groupby = ['File_Name', 'Sector', 'Project_City_Name', 'Date_Key']
    elif sector in ['MACRO', 'IP', 'INFRA']:
        list_columns_groupby = ['File_Name', 'City', 'Date_Key']
    elif sector == 'FDI':
        list_columns_groupby = ['File_Name', 'Date_Key']
    
    if sector == 'RETAIL':
        list_columns_sum = ['NLA', 'Leased_Area', 'New_Supply']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'NLA', 'Leased_Area', 'New_Supply', 'File_Name'
                      ]
    elif sector == 'OFFICE':
        list_columns_sum = ['NLA', 'Leased_Area_End_Of_Q', 'New_Supply']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'NLA', 'Leased_Area_End_Of_Q', 'New_Supply', 'File_Name'
                      ]
    elif sector == 'HOTEL':
        list_columns_sum = ['Total_Supply', 'Leased_Rooms', 'New_Supply']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'Total_Supply', 'Leased_Rooms', 'New_Supply', 'File_Name'
                      ]
    elif sector == 'SA' or sector=='SERVICED_APARTMENT':
        list_columns_sum = ['Total_Supply', 'Leased_Units', 'New_Supply']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'Total_Supply', 'Leased_Units', 'New_Supply', 'File_Name'
                      ]
    elif sector == 'APT' or sector == 'APARTMENT':
        list_columns_sum = ['Total_Launched_Units', 'Total_Sold_Units', 'Available_Units', 'Quarterly_Sales']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'Total_Launched_Units', 'Total_Sold_Units', 'Available_Units', 'Quarterly_Sales', 'File_Name'
                      ]
    elif sector == 'VLTH':
        list_columns_sum = ['Total_Launched_Units', 'Total_Sold_Units', 'Available_Units', 'Quarterly_Sales']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'Total_Launched_Units', 'Total_Sold_Units', 'Available_Units', 'Quarterly_Sales', 'File_Name'
                      ]
    elif sector == 'MACRO':
        if 'Date_key' in data.columns:
            data = data.rename(columns={'Date_key':'Date_Key'})
        else:
            pass
        list_columns_count = ['Indicator']
        re_position = ['Sector', 'City', 'Date_Key', 'Indicator', 'File_Name']
    
    elif sector == 'IP':
        list_columns_count = ['Sub_Project_Name']
        re_position = ['Sector', 'City', 'Date_Key', 'Sub_Project_Name', 'File_Name']
        
    elif sector == 'FDI':
        list_columns_count = ['Project_Name']
        re_position = ['Sector', 'Date_Key', 'Project_Name', 'File_Name']
        
    elif sector == 'INFRA':
        list_columns_count = ['Infratructure_Project']
        re_position = ['Sector', 'City', 'Date_Key', 'Infratructure_Project', 'File_Name']
    else:
        return print(colored('Unknown sector','yellow'))
    
    if sector not in ['IP', 'MACRO', 'FDI', 'INFRA']:
        for i in list_columns_sum:
            if i not in data.columns:
                data[i] = np.nan
                data[i] = data[i].replace({np.nan: None})
            else:
                pass
        list_columns_name = list(merge(list_columns_groupby, list_columns_sum))
        df_flat = pd.DataFrame(columns=list_columns_name)
        df = pd.DataFrame(data.groupby(list_columns_groupby)[list_columns_sum].apply(lambda x : x.astype(float).sum())).reset_index()
        df = df.sort_values(by =['File_Name'],ascending=True).reset_index(drop=True)
        df_count = pd.DataFrame(data.groupby(['File_Name', 'Project_City_Name'])['Date_Key'].count()).reset_index()
        df_count = df_count.sort_values(by =['File_Name'],ascending=True).reset_index(drop=True)
        df['Num_Record'] = df_count['Date_Key']
        df['Sector'] = df['Sector'].apply(lambda x: x.upper() if isinstance(x, str) else x)
        df_flat = pd.concat([df_flat, df], axis=0)
        df_flat = df_flat.reindex(columns=re_position)
        for cols in df_flat.columns:
            for i in df_flat[cols]:
                if type(i) != str:
                    df_flat[cols] = df_flat[cols].replace({None: np.nan})
                    df_flat[cols] = df_flat[cols].fillna(0).astype(int)
        df_flat = df_flat.sort_values(by =['File_Name'],ascending=True).reset_index(drop=True)
        return df_flat
    else:
        list_columns_name = list(merge(list_columns_groupby, list_columns_count))
        df_flat = pd.DataFrame(columns=list_columns_name)
        df = pd.DataFrame(data.groupby(list_columns_groupby)[list_columns_count].count()).reset_index()
        df.insert(loc=0, column='Sector', value= f'{sector}')
        df_flat = pd.concat([df_flat, df], axis=0)
        df_flat = df_flat.reindex(columns=re_position)
        df_flat = df_flat.rename(columns = {' '.join([str(i) for i in list_columns_count]):'Num_Record'})
        df_flat = df_flat.sort_values(by =['File_Name'],ascending=True, ignore_index=False)
        return df_flat"""    

def tracking_flat_file(data, file_url):
    sector = file_url.split('/')[-1].split('_')[0].upper()
    if sector in ['RETAIL', 'OFFICE', 'HOTEL', 'SA', 'SERVICED_APARTMENT', 'APARTMENT', 'APT', 'VLTH', 'PM']:
        list_columns_groupby = ['File_Name', 'Sector', 'Project_City_Name', 'Date_Key']
    elif sector in ['MACRO', 'INFRA', 'IP', 'TENANT']:
        list_columns_groupby = ['File_Name', 'City', 'Date_Key']
    elif sector == 'FDI':
        list_columns_groupby = ['File_Name', 'Date_Key']
    
    if sector == 'RETAIL':
        list_columns_sum = ['NLA', 'Leased_Area', 'New_Supply']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'NLA', 'Leased_Area', 'New_Supply', 'File_Name'
                      ]
    elif sector == 'OFFICE':
        list_columns_sum = ['NLA', 'Leased_Area_End_Of_Q', 'New_Supply']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'NLA', 'Leased_Area_End_Of_Q', 'New_Supply', 'File_Name'
                      ]
    elif sector == 'HOTEL':
        list_columns_sum = ['Total_Supply', 'Leased_Rooms', 'New_Supply']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'Total_Supply', 'Leased_Rooms', 'New_Supply', 'File_Name'
                      ]
    elif sector == 'SA' or sector=='SERVICED_APARTMENT':
        list_columns_sum = ['Total_Supply', 'Leased_Units', 'New_Supply']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'Total_Supply', 'Leased_Units', 'New_Supply', 'File_Name'
                      ]
    elif sector == 'APT' or sector == 'APARTMENT':
        list_columns_sum = ['Total_Launched_Units', 'Total_Sold_Units', 'Available_Units', 'Quarterly_Sales']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'Total_Launched_Units', 'Total_Sold_Units', 'Available_Units', 'Quarterly_Sales', 'File_Name'
                      ]
    elif sector == 'VLTH':
        list_columns_sum = ['Total_Launched_Units', 'Total_Sold_Units', 'Available_Units', 'Quarterly_Sales']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Num_Record'
                       , 'Total_Launched_Units', 'Total_Sold_Units', 'Available_Units', 'Quarterly_Sales', 'File_Name'
                      ]
    elif sector == 'PM':
        list_columns_count = ['Sub_Project_Name']
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'Sub_Project_Name'
                       , 'File_Name'
                      ]
    elif sector == 'MACRO':
        if 'Date_key' in data.columns:
            data = data.rename(columns={'Date_key':'Date_Key'})
        else:
            pass
        list_columns_count = ['Indicator']
        re_position = ['Sector', 'City', 'Date_Key', 'Indicator', 'File_Name']
    
    elif sector in ['IP', 'TENANT']:
        list_columns_count = ['Sub_Project_Name']
        re_position = ['Sector', 'City', 'Date_Key', 'Sub_Project_Name', 'File_Name']
        
    elif sector == 'FDI':
        list_columns_count = ['Project_Name']
        re_position = ['Sector', 'Date_Key', 'Project_Name', 'File_Name']
        
    elif sector == 'INFRA':
        list_columns_count = ['Infratructure_Project']
        re_position = ['Sector', 'City', 'Date_Key', 'Infratructure_Project', 'File_Name']
    else:
        return print(colored('Unknown sector','yellow'))
    
    if sector not in ['IP', 'MACRO', 'FDI', 'INFRA', 'PM', 'TENANT']:
        '''for i in list_columns_sum:
            if i not in data.columns:
                data[i] = np.nan
                data[i] = data[i].replace({np.nan: None})
            else:
                pass'''
        list_columns_name = list(merge(list_columns_groupby, list_columns_sum))
        df_flat = pd.DataFrame(columns=list_columns_name)
        for i in list_columns_sum:
            data[i]=data[i].apply(convert_float)
        df = pd.DataFrame(data.groupby(list_columns_groupby)[list_columns_sum].apply(lambda x : x.astype(float).sum())).reset_index()
        df = df.sort_values(by =['File_Name'],ascending=True).reset_index(drop=True)
        df_count = pd.DataFrame(data.groupby(['File_Name', 'Project_City_Name'])['Date_Key'].count()).reset_index()
        df_count = df_count.sort_values(by =['File_Name'],ascending=True).reset_index(drop=True)
        df['Num_Record'] = df_count['Date_Key']
        df['Date_Key'] = df['Date_Key'].apply(lambda x: int(x) if isinstance(x, str) else x)
        df['Sector'] = df['Sector'].apply(lambda x: x.upper() if isinstance(x, str) else x)
        df_flat = pd.concat([df_flat, df], axis=0)
        df_flat = df_flat.reindex(columns=re_position)
        for cols in df_flat.columns:
            for i in df_flat[cols]:
                if type(i) != str:
                    df_flat[cols] = df_flat[cols].replace({None: np.nan})
                    df_flat[cols] = df_flat[cols].fillna(0).astype(int)
        df_flat = df_flat.sort_values(by =['File_Name'],ascending=True).reset_index(drop=True)
        return df_flat
    else:
        list_columns_name = list(merge(list_columns_groupby, list_columns_count))
        df_flat = pd.DataFrame(columns=list_columns_name)
        df = pd.DataFrame(data.groupby(list_columns_groupby)[list_columns_count].count()).reset_index()
        if 'Sector' not in df.columns:
            df.insert(loc=0, column='Sector', value= f'{sector.upper()}')
        else:
            df['Sector'] = df['Sector'].apply(lambda x: x.upper() if isinstance(x, str) else x)
        df_flat = pd.concat([df_flat, df], axis=0)
        df_flat = df_flat.reindex(columns=re_position)
        df_flat = df_flat.rename(columns = {' '.join([str(i) for i in list_columns_count]):'Num_Record'})
        df_flat = df_flat.sort_values(by =['File_Name'],ascending=True, ignore_index=False)
        return df_flat    
#-------------------------------------------------------
###Check key
"""def Check_Project_Key(data,list_file,relative_url,df_summ_file,BIHub,engine,Sector,file_url):
    flag_key=0
    if 'Project_Key' not in data.columns:
        data['Project_Key'] = np.nan
        data['Project_Key'] = data['Project_Key'].replace({np.nan: None})
    else:
        pass
    if Sector in ['VLTH', 'RETAIL']:
        list_keys = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Type','Project_Key']
        list_keys_on =['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Type']
        if Sector == 'VLTH':
            if 'Branded_Flag' in data.columns:
                list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
                                                  ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
                                                  ,'Project_District_Name','Developer','Date_Key','File_Date'
                                                  ,'Import_Date', 'Brand', 'Branded_Flag'
                                                 ]
            else:
                list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
                                                  ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
                                                  ,'Project_District_Name','Developer','Date_Key','File_Date'
                                                  ,'Import_Date'
                                                 ]
        else:
            list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
                                              ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
                                              ,'Project_District_Name','Developer','Date_Key','File_Date'
                                              ,'Import_Date'
                                             ]
            
    elif Sector in ['OFFICE','APT','APARTMENT']:
        list_keys=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys_on=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade']
        list_column_name_insert_project=['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude','Project_Grade',
                                         'Project_Type', 'Current_Status','Project_Phase', 'Project_City_Name',
                                         'Project_District_Name','Developer','Date_Key','File_Date','Import_Date'
                                        ]
    elif Sector in ['HOTEL','SA','SERVICED_APARTMENT']:
        list_keys=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys_on=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade']
        list_column_name_insert_project=['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude','Project_Grade',
                                         'Current_Status','Project_Phase','Project_City_Name','Project_District_Name',
                                         'Developer','Date_Key','File_Date','Import_Date'
                                        ]
    elif Sector == 'IP':
        list_keys=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type', 'Project_Key']
        list_keys_on=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type']
        list_column_name_insert_project = ['Project_Name','Sub_Project_Name','Latitude','Longtitude'
                                           ,'Phase', 'Zone', 'City', 'District', 'Project_Sub_Type'
                                           , 'Project_Type', 'Developer_Name', 'Date_Key', 'File_Date', 'Import_Date'
                                          ]        

    #Flag 0: key has existed
    #Flag 1: key hasn't existed
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x
    if Sector=='VLTH' or Sector =='RETAIL':
        data = data.rename(columns = {'Sub_Project_Type':'Project_Type'})
        if Sector == 'VLTH':
            if 'Branded_Flag' in data.columns:
                Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project WHERE Branded_Flag = 1",engine)
            else:
                Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
        else:
            Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
        #data[list_keys_on] = data[list_keys_on].applymap(lambda x: str(x).lower())
        #Main_Sector_Project[list_keys] = Main_Sector_Project[list_keys].applymap(lambda x: str(x).lower())
        data[list_keys_on] = data[list_keys_on].applymap(lower_function)
        Main_Sector_Project[list_keys] = Main_Sector_Project[list_keys].applymap(lower_function)
        merged_data=pd.merge(data, Main_Sector_Project[list_keys],
                             left_on = list_keys_on,
                             right_on = list_keys_on,
                             how = 'left'
                            )
        processed_data=merged_data[merged_data['Project_Key_y'].isnull()]
        processed_data = processed_data.rename(columns = {'Project_Status':'Current_Status','Import_date':'Import_Date'})
        
        
    elif Sector=='OFFICE' or Sector=='HOTEL'or Sector=='SA' or Sector=='APT'or Sector=='APARTMENT' or Sector=='SERVICED_APARTMENT':
        data = data.rename(columns = {'Grade':'Project_Grade'})
        Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
        data[list_keys_on] = data[list_keys_on].applymap(lower_function)
        Main_Sector_Project[list_keys] = Main_Sector_Project[list_keys].applymap(lower_function)
        merged_data=pd.merge(data, Main_Sector_Project[list_keys],
                             left_on = list_keys_on,
                             right_on = list_keys_on,
                             how = 'left'
                            )
        processed_data=merged_data[merged_data['Project_Key_y'].isnull()]
        processed_data = processed_data.rename(columns = {'Project_Status':'Current_Status','Import_date':'Import_Date'})
        
        
    elif Sector=='IP':
        Industrial_Park_Project=pd.read_sql("select * from FRESH.Industrial_Park_Project",engine)
        data[list_keys_on] = data[list_keys_on].applymap(lower_function)
        Industrial_Park_Project[list_keys] = Industrial_Park_Project[list_keys].applymap(lower_function)
        merged_data=pd.merge(data, Industrial_Park_Project[list_keys],
                             left_on = list_keys_on,
                             right_on = list_keys_on,
                             how = 'left'
                            )
        processed_data=merged_data[merged_data['Project_Key_y'].isnull()]
        
    for i in list_keys_on:
            data[i]=data[i].str.title()
    if processed_data.count().sum() != 0:
        flag_key=1
        return processed_data, list_column_name_insert_project, flag_key
    
    elif processed_data.count().sum() == 0:
        return processed_data, list_column_name_insert_project, flag_key
    
    else:
        print(colored('Unknown sector in check project key section','yellow'))
        return processed_data, list_column_name_insert_project, flag_key"""

def check_project_key(file_url, data, sector, engine):
    flag_key=0
    if 'Project_Key' not in data.columns:
        data['Project_Key'] = np.nan
        data['Project_Key'] = data['Project_Key'].replace({np.nan: None})
    else:
        pass
    
    if sector in ['VLTH', 'RETAIL']:
        list_keys = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Type','Project_Key']
        list_keys_on =['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Type']

    elif sector in ['OFFICE','APT','APARTMENT','HOTEL','SA','SERVICED_APARTMENT']:
        list_keys=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys_on=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade']
        
    elif sector in ['IP', 'TENANT']:
        list_keys=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type', 'Project_Key']
        list_keys_on=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type']

    if sector not in ['IP', 'TENANT']:
        table_name = 'Main_Sector_Project'
    else:
        table_name = 'Industrial_Park_Project'
    #Flag 0: key has existed
    #Flag 1: key hasn't existed
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x
    if sector in ['VLTH', 'RETAIL']:
        data = data.rename(columns = {'Sub_Project_Type':'Project_Type'})
    elif sector in ['OFFICE','APT','APARTMENT'
                    ,'HOTEL','SA','SERVICED_APARTMENT'
                   ]:
        data = data.rename(columns = {'Grade':'Project_Grade'})    
    #-------------------------------------------------------    
    if sector == 'VLTH':
        if 'Branded_Flag' in data.columns:
            df_query = pd.read_sql(f"select * from FRESH.{table_name} WHERE Branded_Flag = 1",engine)
        else:
            df_query = pd.read_sql(f"select * from FRESH.{table_name}",engine)
    else:
        df_query = pd.read_sql(f"select * from FRESH.{table_name}",engine)
    data[list_keys_on] = data[list_keys_on].applymap(lower_function)
    df_query[list_keys] = df_query[list_keys].applymap(lower_function)
    merged_data = pd.merge(data, df_query[list_keys],
                         left_on = list_keys_on,
                         right_on = list_keys_on,
                         how = 'left'
                        )
    processed_data = merged_data[merged_data['Project_Key_y'].isnull()]
    if sector in ['VLTH', 'RETAIL']:
        processed_data = processed_data.rename(columns = {'Project_Type':'Sub_Project_Type'})
    elif sector in ['OFFICE','APT','APARTMENT'
                    ,'HOTEL','SA','SERVICED_APARTMENT'
                   ]:
        processed_data = processed_data.rename(columns = {'Project_Grade':'Grade'})
    for i in list_keys_on:
        data[i]=data[i].str.title()
    #-------------------------------------------------------
    if processed_data.count().sum() != 0:
        flag_key=1
        return processed_data, flag_key
    elif processed_data.count().sum() == 0:
        return processed_data, flag_key
    else:
        print(colored('Unknown sector in check project key section','yellow'))
        return processed_data, flag_key
    
#-------------------------------------------------------
###Get key after check its existence
"""def Get_Project_Key(Sector, data, processed_data, list_column_name_insert_project, flag_key, engine):
    if Sector in ['VLTH', 'RETAIL']:
        list_keys = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Type','Project_Key']
        list_keys_on =['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Type']

    elif Sector in ['OFFICE','APT','APARTMENT','HOTEL','SA','SERVICED_APARTMENT']:
        list_keys=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys_on=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade']
        
    elif Sector == 'IP':
        list_keys=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type', 'Project_Key']
        list_keys_on=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type']
        
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x       
    if flag_key==1:#Create new key
        list_date_columns = ['File_Date', 'Import_Date']
        if Sector=='VLTH' or Sector =='RETAIL':
            for i in list_date_columns:
                processed_data[i] = pd.to_datetime(processed_data[i]).dt.date
            processed_data['Date_Key'] = pd.to_datetime(processed_data['Date_Key'], format='%Y%m%d')
            processed_data = processed_data.rename(columns={'Sub_Project_Type':'Project_Type', 'Project_Status':'Current_Status'})
            for i in list_keys_on:
                processed_data[i]=processed_data[i].str.title()
            processed_data['Sector']=processed_data['Sector'].str.upper()
            processed_data[list_column_name_insert_project].to_sql('Main_Sector_Project', 
                                                                   engine, 
                                                                   index=False, 
                                                                   if_exists='append', 
                                                                   schema='Fresh'
                                                                  )   
        elif Sector=='OFFICE' or Sector=='HOTEL'or Sector=='SA' or Sector=='APT' or Sector=='APARTMENT' or Sector=='SERVICED_APARTMENT':
            if Sector=='OFFICE' or Sector=='APT' or Sector=='APARTMENT':
                processed_data = processed_data.rename(columns={'Sub_Project_Type':'Project_Type'})
            else:
                pass
            for i in list_date_columns:
                processed_data[i] = pd.to_datetime(processed_data[i]).dt.date
            processed_data['Date_Key'] = pd.to_datetime(processed_data['Date_Key'], format='%Y%m%d')
            processed_data = processed_data.rename(columns={'Grade':'Project_Grade', 'Project_Status':'Current_Status'})
            for i in list_keys_on:
                processed_data[i]=processed_data[i].str.title()
            processed_data['Sector']=processed_data['Sector'].str.upper()
            processed_data[list_column_name_insert_project].to_sql('Main_Sector_Project', 
                                                                   engine, 
                                                                   index=False, 
                                                                   if_exists='append', 
                                                                   schema='Fresh'
                                                                  )    
        elif Sector=='IP':
            processed_data['File_Date'] = pd.to_datetime(processed_data['File_Date']).dt.date
            for i in list_keys_on:
                processed_data[i]=processed_data[i].str.title()
            processed_data[list_column_name_insert_project].to_sql('Industrial_Park_Project', 
                                                                   engine, 
                                                                   index=False, 
                                                                   if_exists='append', 
                                                                   schema='Fresh'
                                                                  )
        #Get key
        if Sector=='VLTH' or Sector =='RETAIL':
            data.rename(columns = {'Sub_Project_Type':'Project_Type'}, inplace = True)
            if Sector == 'VLTH':
                if 'Branded_Flag' in data.columns:
                    Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project WHERE Branded_Flag = 1",engine)
                else:
                    Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
            else:
                Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
            data[list_keys_on] = data[list_keys_on].applymap(lower_function)
            Main_Sector_Project[list_keys] = Main_Sector_Project[list_keys].applymap(lower_function)
            data=pd.merge(data, Main_Sector_Project[list_keys],
                                 left_on = list_keys_on,
                                 right_on = list_keys_on,
                                 how = 'left'
                                )
            data.drop(columns='Project_Key_x', axis=1,inplace=True)
            for i in list_keys_on:
                data[i]=data[i].str.title()
            data.rename(columns = {'Project_Key_y':'Project_Key','Project_Type':'Sub_Project_Type'}, inplace = True)
            data['Sector']=data['Sector'].str.upper()
            return data
        
        elif Sector=='OFFICE' or Sector=='HOTEL'or Sector=='SA' or Sector=='APT'or Sector=='APARTMENT' or Sector=='SERVICED_APARTMENT':
            data.rename(columns = {'Grade':'Project_Grade'}, inplace = True)
            Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
            data[list_keys_on] = data[list_keys_on].applymap(lower_function)
            Main_Sector_Project[list_keys] = Main_Sector_Project[list_keys].applymap(lower_function)
            data=pd.merge(data, Main_Sector_Project[list_keys],
                                 left_on = list_keys_on,
                                 right_on = list_keys_on,
                                 how = 'left'
                                )
            data.drop(columns='Project_Key_x', axis=1,inplace=True)
            for i in list_keys_on:
                data[i]=data[i].str.title()
            data.rename(columns = {'Project_Key_y':'Project_Key','Project_Grade':'Grade'}, inplace = True)
            data['Sector']=data['Sector'].str.upper()
            return data
        
        elif Sector == 'IP':
            Industrial_Park_Project=pd.read_sql("select * from FRESH.Industrial_Park_Project",engine)
            data[list_keys_on] = data[list_keys_on].applymap(lower_function)
            Industrial_Park_Project[list_keys] = Industrial_Park_Project[list_keys].applymap(lower_function)
            data=pd.merge(data, Industrial_Park_Project[list_keys],
                                 left_on = list_keys_on,
                                 right_on = list_keys_on,
                                 how = 'left'
                                )
            data.drop(columns='Project_Key_x', axis=1,inplace=True)
            for i in list_keys_on:
                data[i]=data[i].str.title()
            data.rename(columns = {'Project_Key_y':'Project_Key'}, inplace = True)
            data['File_Date'] = pd.to_datetime(data['File_Date']).dt.date#Chỉnh lại chỗ format này cho đòng nhất với logic ở file update
            return data
            
    elif flag_key==0:
        if Sector=='VLTH' or Sector =='RETAIL':
            data.rename(columns = {'Sub_Project_Type':'Project_Type'}, inplace = True)
            if Sector == 'VLTH':
                if 'Branded_Flag' in data.columns:
                    Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project WHERE Branded_Flag = 1",engine)
                else:
                    Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
            else:
                Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
            data[list_keys_on] = data[list_keys_on].applymap(lower_function)
            Main_Sector_Project[list_keys] = Main_Sector_Project[list_keys].applymap(lower_function)
            data=pd.merge(data, Main_Sector_Project[list_keys],
                                 left_on = list_keys_on,
                                 right_on = list_keys_on,
                                 how = 'left'
                                )
            data.drop(columns='Project_Key_x', axis=1,inplace=True)
            for i in list_keys_on:
                data[i]=data[i].str.title()
            data['Sector']=data['Sector'].str.upper()
            data.rename(columns = {'Project_Key_y':'Project_Key','Project_Type':'Sub_Project_Type'}, inplace = True)
            return data
        
        elif Sector=='OFFICE' or Sector=='HOTEL'or Sector=='SA' or Sector=='APT'or Sector=='APARTMENT' or Sector=='SERVICED_APARTMENT':
            data.rename(columns = {'Grade':'Project_Grade'}, inplace = True)
            Main_Sector_Project=pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
            data[list_keys_on] = data[list_keys_on].applymap(lower_function)
            Main_Sector_Project[list_keys] = Main_Sector_Project[list_keys].applymap(lower_function)
            data=pd.merge(data, Main_Sector_Project[list_keys],
                                 left_on = list_keys_on,
                                 right_on = list_keys_on,
                                 how = 'left'
                                )
            data.drop(columns='Project_Key_x', axis=1,inplace=True)
            for i in list_keys_on:
                data[i]=data[i].str.title()
            data['Sector']=data['Sector'].str.upper()
            data.rename(columns = {'Project_Key_y':'Project_Key','Project_Grade':'Grade'}, inplace = True)
            return data
        
        elif Sector == 'IP':
            Industrial_Park_Project=pd.read_sql("select * from FRESH.Industrial_Park_Project",engine)
            data[list_keys_on] = data[list_keys_on].applymap(lower_function)
            Industrial_Park_Project[list_keys] = Industrial_Park_Project[list_keys].applymap(lower_function)
            data=pd.merge(data, Industrial_Park_Project[list_keys],
                                 left_on = list_keys_on,
                                 right_on = list_keys_on,
                                 how = 'left'
                                )
            data.drop(columns='Project_Key_x', axis=1,inplace=True)
            for i in list_keys_on:
                data[i]=data[i].str.title()
            data.rename(columns = {'Project_Key_y':'Project_Key'}, inplace = True)
            data['File_Date'] = pd.to_datetime(data['File_Date']).dt.date
            return data       
        
    else:
        print(colored('Unknown sector in get key section','yellow'))
        return data"""

def get_project_key(flag_key, processed_data, data, sector, engine):
    if sector in ['VLTH', 'RETAIL']:
        list_keys = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Type','Project_Key']
        list_keys_on =['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Type']
        if sector == 'VLTH':
            if 'Brand' in data.columns:
                list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
                                                  ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
                                                  ,'Project_District_Name','Developer','Date_Key','File_Date'
                                                  ,'Import_Date', 'Brand'
                                                 ]
            else:
                list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
                                                  ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
                                                  ,'Project_District_Name','Developer','Date_Key','File_Date'
                                                  ,'Import_Date'
                                                 ]
        else:
            list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
                                              ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
                                              ,'Project_District_Name','Developer','Date_Key','File_Date'
                                              ,'Import_Date'
                                             ]
            
    elif sector in ['OFFICE','APT','APARTMENT']:
        list_keys=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys_on=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade']
        list_column_name_insert_project=['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude','Project_Grade',
                                         'Project_Type', 'Current_Status','Project_Phase', 'Project_City_Name',
                                         'Project_District_Name','Developer','Date_Key','File_Date','Import_Date'
                                        ]
    elif sector in ['HOTEL','SA','SERVICED_APARTMENT']:
        list_keys=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys_on=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade']
        list_column_name_insert_project=['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude','Project_Grade',
                                         'Current_Status','Project_Phase','Project_City_Name','Project_District_Name',
                                         'Developer','Date_Key','File_Date','Import_Date'
                                        ]
    elif sector in ['IP', 'TENANT']:
        list_keys=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type', 'Project_Key']
        list_keys_on=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type']
        list_column_name_insert_project = ['Project_Name','Sub_Project_Name','Latitude','Longtitude'
                                           ,'Phase', 'Zone', 'City', 'District', 'Project_Sub_Type'
                                           , 'Project_Type', 'Developer_Name', 'Date_Key', 'File_Date', 'Import_Date'
                                          ]
    if sector not in ['IP', 'TENANT']:
        table_name = 'Main_Sector_Project'
    else:
        table_name = 'Industrial_Park_Project'
    #-------------------------------------------------------  
    #Flag 0: key has existed
    #Flag 1: key hasn't existed
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x   
    #create new key
    if flag_key==1:
        if sector != 'IP':
            list_date_columns = ['File_Date', 'Import_Date']
            for i in list_date_columns:
                processed_data[i] = pd.to_datetime(processed_data[i]).dt.date
            processed_data['Date_Key'] = pd.to_datetime(processed_data['Date_Key'], format='%Y%m%d')
        else:
            processed_data['File_Date'] = pd.to_datetime(processed_data['File_Date']).dt.date
        #------------------------------------------------------- 
        if sector in ['VLTH', 'RETAIL']:
            processed_data = processed_data.rename(columns={'Sub_Project_Type':'Project_Type', 'Project_Status':'Current_Status'})
        elif sector in ['OFFICE','APT','APARTMENT','HOTEL','SA','SERVICED_APARTMENT']:
            if sector in ['OFFICE', 'APT', 'APARTMENT']:
                processed_data = processed_data.rename(columns={'Sub_Project_Type':'Project_Type'})
            else:
                pass
            processed_data = processed_data.rename(columns={'Grade':'Project_Grade', 'Project_Status':'Current_Status'})    
        for i in list_keys_on:
            processed_data[i]=processed_data[i].str.title()
        if sector != 'IP':
            processed_data['Sector'] = processed_data['Sector'].str.upper()
        else:
            pass
        #-------------------------------------------------------     
        processed_data[list_column_name_insert_project].to_sql(f'{table_name}', 
                                                               engine, 
                                                               index=False, 
                                                               if_exists='append', 
                                                               schema='FRESH'
                                                              )
        #Get key
        if sector in ['VLTH', 'RETAIL']:
            data = data.rename(columns = {'Sub_Project_Type':'Project_Type'})
        elif sector in ['OFFICE','APT','APARTMENT'
                        ,'HOTEL','SA','SERVICED_APARTMENT'
                       ]:
            data = data.rename(columns = {'Grade':'Project_Grade'})    
        #-------------------------------------------------------    
        if sector == 'VLTH':
            if 'Branded_Flag' in data.columns:
                df_query = pd.read_sql(f"select * from FRESH.{table_name} WHERE Branded_Flag = 1",engine)
            else:
                df_query = pd.read_sql(f"select * from FRESH.{table_name}",engine)
        else:
            df_query = pd.read_sql(f"select * from FRESH.{table_name}",engine)
        data[list_keys_on] = data[list_keys_on].applymap(lower_function)
        df_query[list_keys] = df_query[list_keys].applymap(lower_function)
        data = pd.merge(data, df_query[list_keys],
                             left_on = list_keys_on,
                             right_on = list_keys_on,
                             how = 'left'
                            )
        data.drop(columns='Project_Key_x', axis=1,inplace=True)
        for i in list_keys_on:
                data[i]=data[i].str.title()
        #------------------------------------------------------- 
        if sector in ['VLTH', 'RETAIL']:
            data.rename(columns = {'Project_Key_y':'Project_Key','Project_Type':'Sub_Project_Type'}, inplace = True)
            data['Sector']=data['Sector'].str.upper()
        elif sector in ['OFFICE','APT','APARTMENT'
                        ,'HOTEL','SA','SERVICED_APARTMENT'
                       ]:
            data.rename(columns = {'Project_Key_y':'Project_Key','Project_Grade':'Grade'}, inplace = True)
            data['Sector']=data['Sector'].str.upper()
        else:
            data.rename(columns = {'Project_Key_y':'Project_Key'}, inplace = True)
            data['File_Date'] = pd.to_datetime(data['File_Date']).dt.date
        return data
        #-------------------------------------------------------
    #Get key
    elif flag_key==0:
        if sector in ['VLTH', 'RETAIL']:
            data = data.rename(columns = {'Sub_Project_Type':'Project_Type'})
        elif sector in ['OFFICE','APT','APARTMENT'
                        ,'HOTEL','SA','SERVICED_APARTMENT'
                       ]:
            data = data.rename(columns = {'Grade':'Project_Grade'})    
        #-------------------------------------------------------    
        if sector == 'VLTH':
            if 'Branded_Flag' in data.columns:
                df_query = pd.read_sql(f"select * from FRESH.{table_name} WHERE Branded_Flag = 1",engine)
            else:
                df_query = pd.read_sql(f"select * from FRESH.{table_name}",engine)
        else:
            df_query = pd.read_sql(f"select * from FRESH.{table_name}",engine)
        data[list_keys_on] = data[list_keys_on].applymap(lower_function)
        df_query[list_keys] = df_query[list_keys].applymap(lower_function)
        data = pd.merge(data, df_query[list_keys],
                             left_on = list_keys_on,
                             right_on = list_keys_on,
                             how = 'left'
                            )
        data.drop(columns='Project_Key_x', axis=1,inplace=True)
        for i in list_keys_on:
                data[i]=data[i].str.title()
        #------------------------------------------------------- 
        if sector in ['VLTH', 'RETAIL']:
            data.rename(columns = {'Project_Key_y':'Project_Key','Project_Type':'Sub_Project_Type'}, inplace = True)
            data['Sector']=data['Sector'].str.upper()
        elif sector in ['OFFICE','APT','APARTMENT'
                        ,'HOTEL','SA','SERVICED_APARTMENT'
                       ]:
            data.rename(columns = {'Project_Key_y':'Project_Key','Project_Grade':'Grade'}, inplace = True)
            data['Sector']=data['Sector'].str.upper()
        elif sector == 'IP':
            data.rename(columns = {'Project_Key_y':'Project_Key'}, inplace = True)
            data['File_Date'] = pd.to_datetime(data['File_Date']).dt.date
        else:
            data.rename(columns = {'Project_Key_y':'Project_Key'}, inplace = True)
        return data
        
    else:
        print(colored('Unknown sector in get key section','yellow'))
        return data
#-------------------------------------------------------
###Insert to DB            
"""def Insert_to_fresh(sector,cnxn,data,file_url):
    conn ='Driver={ODBC Driver 17 for SQL Server};Server=tcp:hkazdevsqld3vnreserch.database.windows.net,1433;Database=D3VNResearch_Staging;Uid=D3VNResearch@savills.com.vn;Pwd=!Advisory082022@;Encrypt=yes;Authentication=ActiveDirectoryPassword;Connection Timeout=30;'
    quoted = quote_plus(conn)
    engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        file_name = file_url.split('/')[-1].split('.')[0]
        print("Insert {} to Fresh".format(file_name))
        if executemany:
            cursor.fast_executemany = True
            
    if sector=='RETAIL':
        table_name='RETAIL'
        list_float_columns = ['Longtitude','Latitude','Service_Charge','Avg_Gross_Rent','Leased_Area',
                             'Vaccant_Area','NLA','GFA','Number_Of_Floors','New_Supply'
                             ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='OFFICE':
        table_name='OFFICE'
        list_float_columns=['Longtitude','Latitude', 'Gross_Office_Area', 'Efficiency', 'NLA',
                           'Leased_Area_End_Of_Q', 'Vacant_Area', 'Average_Price',
                           'Avg_Gross_Rent', 'Avg_Net_Rent','New_Supply'
                           ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)    
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')        
            
    elif sector=='HOTEL':
        table_name='HOTEL'
        data.rename(columns = {'Project_Type':'Sub_Project_Type'}, inplace = True)
        list_float_columns=['Longtitude','Latitude', 'Land_Area', 'Occupancy','Total_Supply',
                            'Leased_Rooms','Avg_Room_Price', 'Service_Charge', 'Quoted_Room_Rate', 'New_Supply'
                           ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True) 
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='SERVICED_APARTMENT' or sector=='SA':
        table_name='Serviced_Apartment'
        list_float_columns=['Latitude', 'Longtitude', 'Total_Supply', 'Launch_Units', 'Leased_Units', 
                            'Net_Lettable', 'Rev_PAU', 'Rev_PAU_m2','Avg_Rent', 'Rent_Per_Unit', 
                            'Avg_Unit_Size', 'New_Supply','Vacant_Units'
                           ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='APARTMENT' or sector=='APT':
        table_name='APT'
        for i in data.columns:
            if i in ['Area_GFA_Max','Area_GFA_Min']:
                old_names = ['Area_NFA_Max', 'Area_NFA_Min', 'Area_GFA_Max', 'Area_GFA_Min',
                            'NFA_Primary_Price', 'CFA_Primary_Price', 'NFA_Secondary_Price', 'CFA_Secondary_Price'
                            ]
                new_names = ['Area_NSA_Max', 'Area_NSA_Min', 'Area_NFA_Max', 'Area_NFA_Min',
                            'NSA_Primary_Price', 'NFA_Primary_Price', 'NSA_Secondary_Price', 'NFA_Secondary_Price'
                            ]
                data.rename(columns=dict(zip(old_names, new_names)), inplace=True)
                break
            else:
                pass
        list_float_columns = ['Land_Area', 'Min_Size_Area', 'Max_Size_Area', 'Latitude', 'Longtitude', 
                             'Secondary_Supply', 'Total_Supply', 'Newly_Launch', 'Total_Launched_Units', 'Available_Units', 
                             'Total_Sold_Units', 'Sold_Units_End_Of_Q', 'Quarterly_Sales', 'Area_NSA_Max', 'Area_NSA_Min', 
                             'Area_NFA_Max', 'Area_NFA_Min', 'NSA_Primary_Price', 'NFA_Primary_Price',
                             'NSA_Secondary_Price', 'NFA_Secondary_Price', 'Quarterly_Sales_New_Supply', 
                             'Remaining_Supply', 'Avg_Asking_Rent', 'Avg_Unit_Rent'
                             ]     
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True) 
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='VLTH':
        table_name='VLTH'
        list_float_columns = ['Land_Area', 'Latitude', 'Longtitude', 'Total_Supply', 'Newly_Launch', 
                             'Total_Launched_Units', 'Available_Units', 'Total_Sold_Units', 'Sold_Units_End_Of_Q', 
                             'Quarterly_Sales', 'Remaining_Supply', 'LA_Primary_Price', 'UNIT_Primary_Price', 
                             'LA_Secondary_Price', 'UNIT_Secondary_Price', 'Quarterly_Sales_New_Supply'
                             ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')

    elif sector=='IP':
        table_name='Industrial_Park'
        list_float_columns = ['LU_Residental', 'LU_Administration', 'LU_Green_Water', 'LU_Transportation', 'LU_Infra'
                              , 'LU_Commercial', 'LU_Factory_Building', 'LU_Warehouse', 'LU_Other'
                              , 'PROJECT_Land_Area', 'LAND_Leaseable_Land_Area', 'LAND_Available_Land_Area', 'LAND_Leased_Land_Area'
                              , 'RB_Land_Area', 'RB_Total_GFA', 'RB_NLA', 'RB_Leased_Area'
                              , 'LAND_AVG_Rent_Primary_Price', 'LAND_AVG_Rent_Secondary_Price', 'LAND_AVG_Rent_Actual'
                              , 'LAND_AVG_Rent_Last_Transaction', 'RB_AVG_Rent_Primary_Price', 'RB_AVG_Rent_Secondary_Price'
                              , 'RB_AVG_Rent_Actual', 'RB_AVG_Rent_Last_Transaction'
                             ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector == 'MACRO':
        table_name = 'MACRO'
        for i in data.columns:
            if i == 'Date_Import' or i == 'Date_import':
                data = data.rename(columns = {i:'Input_Date'})
            else:
                pass
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector == 'FDI':
        table_name = 'FDI'
        list_float_columns = ['Registered_FDI', 'Additional_FDI'
                             ]
        list_str_columns = ['Business_Name', 'Investor_Name'
                           ]
        list_date_columns = ['Registered_Date', 'Additional_FDI_Date'
                            ]
        for i in list_float_columns:
            data[i] = data[i].apply(convert_float).replace('\.0$','', regex=True)
        for i in list_str_columns:
            data[i] = data[i].apply(lambda x: x.title() if isinstance(x, str) else x)
        for i in list_date_columns:
            data[i] = pd.to_datetime(data[i],format='%Y-%m-%d', errors='coerce')
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector == 'INFRA':
        table_name = 'Infrastructure'
        list_str_columns = ['Infratructure_Project']
        for i in list_str_columns:
            data[i] = data[i].apply(lambda x: x.title() if isinstance(x, str) else x)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')"""

def insert_to_fresh(file_url, data, cnt_str):
    # conn ='Driver={ODBC Driver 17 for SQL Server};Server=tcp:hkazdevsqld3vnreserch.database.windows.net,1433;Database=D3VNResearch_Staging;Uid=D3VNResearch@savills.com.vn;Pwd=@Advisory032023!;Encrypt=yes;Authentication=ActiveDirectoryPassword;Connection Timeout=30;'
    # quoted = quote_plus(conn)
    # engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

    engine = create_engine(cnt_str)
    #Test Connection
    try:
        result = engine.execute("SELECT 1")
        print("CONNECTION SUCESSFUL IN INSERT TO FRESH FUNCTION!")
    except Exception as e:
        print("CONNECTION FAILED IN INSERT TO FRESH FUNCTION!:",str(e))

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        file_name = file_url.split('/')[-1].split('.')[0]
        print("Insert {} to Fresh".format(file_name))
        if executemany:
            cursor.fast_executemany = True
    sector = file_url.split('/')[-1].split('_')[0].upper()
    #------------------------------------------------------- 
    if sector=='RETAIL':
        table_name='RETAIL'
        list_float_columns = ['Longtitude','Latitude','Service_Charge','Avg_Gross_Rent','Leased_Area',
                             'Vacant_Area','NLA','GFA','Number_Of_Floors','New_Supply'
                             ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='OFFICE':
        table_name='OFFICE'
        list_float_columns=['Longtitude','Latitude', 'Gross_Office_Area', 'Efficiency', 'NLA',
                           'Leased_Area_End_Of_Q', 'Vacant_Area', 'Average_Price',
                           'Avg_Gross_Rent', 'Avg_Net_Rent','New_Supply'
                           ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)    
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')        
            
    elif sector=='HOTEL':
        table_name='HOTEL'
        data.rename(columns = {'Project_Type':'Sub_Project_Type'}, inplace = True)
        list_float_columns=['Longtitude','Latitude', 'Land_Area', 'Occupancy','Total_Supply',
                            'Leased_Rooms','Avg_Room_Price', 'Service_Charge', 'Quoted_Room_Rate', 'New_Supply'
                           ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True) 
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='SERVICED_APARTMENT' or sector=='SA':
        table_name='Serviced_Apartment'
        for i in data.columns:
            if i in ['Rev_PAR']:
                old_names = ['Rev_PAR'
                            ]
                new_names = ['Rev_PAU'
                            ]
                data.rename(columns=dict(zip(old_names, new_names)), inplace=True)
                break
            else:
                pass
        list_float_columns=['Latitude', 'Longtitude', 'Total_Supply', 'Launch_Units', 'Leased_Units', 
                            'Net_Lettable', 'Rev_PAU', 'Rev_PAU_m2','Avg_Rent', 'Rent_Per_Unit', 
                            'Avg_Unit_Size', 'New_Supply','Vacant_Units'
                           ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='APARTMENT' or sector=='APT':
        table_name='APT'
        for i in data.columns:
            if i in ['Area_GFA_Max','Area_GFA_Min']:
                old_names = ['Area_NFA_Max', 'Area_NFA_Min', 'Area_GFA_Max', 'Area_GFA_Min',
                            'NFA_Primary_Price', 'CFA_Primary_Price', 'NFA_Secondary_Price', 'CFA_Secondary_Price'
                            ]
                new_names = ['Area_NSA_Max', 'Area_NSA_Min', 'Area_NFA_Max', 'Area_NFA_Min',
                            'NSA_Primary_Price', 'NFA_Primary_Price', 'NSA_Secondary_Price', 'NFA_Secondary_Price'
                            ]
                data.rename(columns=dict(zip(old_names, new_names)), inplace=True)
                break
            else:
                pass
        list_float_columns = ['Land_Area', 'Min_Size_Area', 'Max_Size_Area', 'Latitude', 'Longtitude', 
                             'Secondary_Supply', 'Total_Supply', 'Newly_Launch', 'Total_Launched_Units', 'Available_Units', 
                             'Total_Sold_Units', 'Sold_Units_End_Of_Q', 'Quarterly_Sales', 'Area_NSA_Max', 'Area_NSA_Min', 
                             'Area_NFA_Max', 'Area_NFA_Min', 'NSA_Primary_Price', 'NFA_Primary_Price',
                             'NSA_Secondary_Price', 'NFA_Secondary_Price', 'Quarterly_Sales_New_Supply', 
                             'Remaining_Supply', 'Avg_Asking_Rent', 'Avg_Unit_Rent'
                             ]     
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True) 
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='VLTH':
        table_name='VLTH'
        list_float_columns = ['Land_Area', 'Latitude', 'Longtitude', 'Total_Supply', 'Newly_Launch', 
                             'Total_Launched_Units', 'Available_Units', 'Total_Sold_Units', 'Sold_Units_End_Of_Q', 
                             'Quarterly_Sales', 'Remaining_Supply', 'LA_Primary_Price', 'UNIT_Primary_Price', 
                             'LA_Secondary_Price', 'UNIT_Secondary_Price', 'Quarterly_Sales_New_Supply'
                             ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')

    elif sector=='IP':
        table_name='Industrial_Park'
        list_float_columns = ['LU_Residental', 'LU_Administration', 'LU_Green_Water', 'LU_Transportation', 'LU_Infra'
                              , 'LU_Commercial', 'LU_Factory_Building', 'LU_Warehouse', 'LU_Other'
                              , 'PROJECT_Land_Area', 'LAND_Leaseable_Land_Area', 'LAND_Available_Land_Area', 'LAND_Leased_Land_Area'
                              , 'RB_Land_Area', 'RB_Total_GFA', 'RB_NLA', 'RB_Leased_Area'
                              , 'LAND_AVG_Rent_Primary_Price', 'LAND_AVG_Rent_Secondary_Price', 'LAND_AVG_Rent_Actual'
                              , 'LAND_AVG_Rent_Last_Transaction', 'RB_AVG_Rent_Primary_Price', 'RB_AVG_Rent_Secondary_Price'
                              , 'RB_AVG_Rent_Actual', 'RB_AVG_Rent_Last_Transaction'
                             ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector == 'MACRO':
        table_name = 'MACRO'
        for i in data.columns:
            if i == 'Date_Import' or i == 'Date_import':
                data = data.rename(columns = {i:'Input_Date'})
            else:
                pass
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector == 'FDI':
        table_name = 'FDI'
        list_float_columns = ['Registered_FDI', 'Additional_FDI'
                             ]
        list_str_columns = ['Business_Name', 'Investor_Name'
                           ]
        list_date_columns = ['Registered_Date', 'Additional_FDI_Date'
                            ]
        for i in list_float_columns:
            data[i] = data[i].apply(convert_float).replace('\.0$','', regex=True)
        for i in list_str_columns:
            data[i] = data[i].apply(lambda x: x.title() if isinstance(x, str) else x)
        for i in list_date_columns:
            data[i] = pd.to_datetime(data[i],format='%Y-%m-%d', errors='coerce')
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector == 'INFRA':
        table_name = 'Infrastructure'
        list_str_columns = ['Infratructure_Project']
        for i in list_str_columns:
            data[i] = data[i].apply(lambda x: x.title() if isinstance(x, str) else x)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector == 'PM':
        sub_sector = file_url.split('/')[-1].split('_')[1].upper()
        if sub_sector in ['APT', 'APARTMENT']:
            table_name = 'Product_Mix_Apartment'
            list_float_columns = ['Total_Supply', 'Area_NFA_Min', 'Area_NFA_Max', 'Area_NSA_Min', 'Area_NSA_Max',
                                  'NFA_Primary_Price', 'NSA_Primary_Price', 'NSA_Secondary_Price', 'NFA_Secondary_Price'
                                 ]
        else:
            table_name = 'Product_Mix_VLTH'
            list_float_columns = ['Total_Supply', 'LA_Min', 'LA_Max', 'GFA_Min', 'GFA_Max',
                                  'LA_Primary_Price', 'GFA_Primary_Price', 'LA_Secondary_Price', 'GFA_Secondary_Price'
                                 ]
        for i in list_float_columns:
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector == 'TENANT':
        table_name='Industrial_Park_Tenant'
        list_float_columns = ['Acquired_Area']
        for i in list_float_columns:
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')

#-------------------------------------------------------
###Create tracking dataframe for values in DB        
def tracking_db(df_flat, engine):
    sector=df_flat['Sector'].iloc[0].upper()
    file_name=df_flat['File_Name'].tolist()
    file_name=f"({str(file_name)[1:-1]})"
    if sector == 'RETAIL':
        df_query = pd.read_sql('''
                        WITH temp_tracking_retail AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.RETAIL t
                        )
                            SELECT 'RETAIL' as Sector, t.Project_City_Name, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record, SUM(t.NLA) NLA, SUM(t.Leased_Area) Leased_Area
                                , SUM(t.New_Supply) New_Supply, t.File_Name
                            FROM temp_tracking_retail t
                            JOIN FRESH.Calendar f on t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name in {}
                            GROUP BY t.File_Name, t.Project_City_Name, f.DATE_KEY
                             '''.format(file_name),engine)
    elif sector == 'OFFICE':
        df_query = pd.read_sql('''
                        WITH temp_tracking_office AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.OFFICE t
                        )
                            SELECT 'OFFICE' as Sector, t.Project_City_Name, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record, SUM(t.NLA) NLA, SUM(t.Leased_Area_End_Of_Q) Leased_Area_End_Of_Q
                                , SUM(t.New_Supply) New_Supply, t.File_Name
                            FROM temp_tracking_office t
                                JOIN FRESH.Calendar f ON t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1	AND t.File_Name in {}
                            GROUP BY t.File_Name, t.Project_City_Name, f.DATE_KEY
                             '''.format(file_name),engine)
    elif sector == 'HOTEL':
        df_query = pd.read_sql('''
                        WITH temp_tracking_hotel AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.HOTEL t
                        )
                            SELECT 'HOTEL' AS Sector, t.Project_City_Name, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record, SUM(t.Total_Supply) Total_Supply, SUM(t.Leased_Rooms) Leased_Rooms
                                , SUM(t.New_Supply) New_Supply, t.File_Name
                            fROM temp_tracking_hotel t
                                JOIN FRESH.Calendar f ON t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name IN {}
                            GROUP BY t.File_Name, t.Project_City_Name, f.DATE_KEY
                             '''.format(file_name),engine)
    elif sector == 'SERVICED_APARTMENT':
        df_query = pd.read_sql('''
                        WITH temp_tracking_sa AS
                        (
                            SELECT t.*
                            , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.Serviced_Apartment t
                        )
                            SELECT 'SERVICED_APARTMENT' AS Sector, t.Project_City_Name, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record, SUM(t.Total_Supply) Total_Supply, SUM(t.Leased_Units) Leased_Units
                                , SUM(t.New_Supply) New_Supply, t.File_Name
                            FROM temp_tracking_sa t
                                JOIN FRESH.Calendar f ON t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name IN {}
                            GROUP BY t.File_Name, t.Project_City_Name, f.DATE_KEY
                             '''.format(file_name),engine)
    elif sector == 'APARTMENT':
        df_query = pd.read_sql('''
                        WITH temp_tracking_apt AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.APT t
                        )
                            SELECT 'APARTMENT' AS Sector, t.Project_City_Name, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record, SUM(t.Total_Launched_Units) Total_Launched_Units, SUM(t.Total_Sold_Units) Total_Sold_Units
                                , SUM(t.Available_Units) Available_Units, SUM(t.Quarterly_Sales) Quarterly_Sales
                                , t.File_Name
                            FROM temp_tracking_apt t
                                JOIN FRESH.Calendar f on t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name IN {}
                            GROUP BY t.File_Name, t.Project_City_Name, f.DATE_KEY
                             '''.format(file_name),engine)
    elif sector == 'VLTH':
        df_query = pd.read_sql('''
                        WITH temp_tracking_vlth AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.VLTH t
                        )
                            SELECT 'VLTH' AS Sector, t.Project_City_Name, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record, SUM(t.Total_Launched_Units) Total_Launched_Units, SUM(t.Total_Sold_Units) Total_Sold_Units
                                , SUM(t.Available_Units) Available_Units, SUM(t.Quarterly_Sales) Quarterly_Sales
                                , t.File_Name
                            FROM temp_tracking_vlth t
                                JOIN FRESH.Calendar f on t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name IN {}
                            GROUP BY t.File_Name, t.Project_City_Name, f.DATE_KEY
                             '''.format(file_name),engine)
    elif sector == 'IP':
        df_query = pd.read_sql('''
                        WITH temp_tracking_ip AS
                        (
                        SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                        FROM FRESH.Industrial_Park t
                        )
                        SELECT 'IP' as Sector, t.City, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record
                                , t.File_Name
                        FROM temp_tracking_ip t
                                JOIN FRESH.Calendar f ON t.Date_Key = f.DATE_KEY
                        WHERE t.Rn = 1 AND t.File_Name IN {}
                        GROUP BY t.File_Name, t.City, f.DATE_KEY
                             '''.format(file_name),engine)
    elif sector == 'MACRO':
        df_query = pd.read_sql('''
                        WITH temp_tracking_macro AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.MACRO t
                        )
                            SELECT 'MACRO' as Sector, t.City, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record
                                , t.File_Name
                            FROM temp_tracking_macro t
                                JOIN FRESH.Calendar f ON t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name IN {}
                            GROUP BY t.File_Name, t.City, f.DATE_KEY
                             '''.format(file_name),engine)
    elif sector == 'FDI':
        df_query = pd.read_sql('''
                        WITH temp_tracking_fdi AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name, t.Date_Key ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.FDI t
                        )
                            SELECT 'FDI' as Sector, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record
                                , t.File_Name
                            FROM temp_tracking_fdi t
                                JOIN FRESH.Calendar f ON t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name IN {}
                            GROUP BY t.File_Name, f.DATE_KEY
                                 '''.format(file_name),engine)
    elif sector == 'INFRA':
        df_query = pd.read_sql('''
                        WITH temp_tracking_infra AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.Infrastructure t
                        )
                            SELECT 'INFRA' as Sector, t.City, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record
                                , t.File_Name
                            FROM temp_tracking_infra t
                                JOIN FRESH.Calendar f ON t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name IN {}
                            GROUP BY t.File_Name, t.City, f.DATE_KEY
                             '''.format(file_name),engine)   
    elif sector == 'TENANT':
        df_query = pd.read_sql('''
                        WITH temp_tracking_tenant AS
                        (
                            SELECT t.*
                                , DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY t.Import_date DESC) Rn
                            FROM FRESH.Industrial_Park_Tenant t
                        )
                            SELECT 'TENANT' as Sector, t.City, f.DATE_KEY Date_Key
                                , COUNT(*) Num_Record
                                , t.File_Name
                            FROM temp_tracking_tenant t
                                JOIN FRESH.Calendar f ON t.Date_Key = f.DATE_KEY
                            WHERE t.Rn = 1 AND t.File_Name IN {}
                            GROUP BY t.File_Name, t.City, f.DATE_KEY
                             '''.format(file_name),engine)
    else:
        print(colored('Check df_flat - upper sector?', 'yellow'))
    
    for cols in df_query.columns:
        for i in df_query[cols]:
            if type(i) != str:
                df_query[cols] = df_query[cols].replace({None: np.nan})
                df_query[cols] = df_query[cols].fillna(0).astype(int)
    df_query = df_query.sort_values(by =['File_Name'],ascending=True).reset_index(drop=True)
    return df_query    
#------------------------------------------------------- 
#Process to create content of email
def highlight_diff(data, other):
    attr = 'background-color: #E85A24; color:white'
    return pd.DataFrame(np.where(data.ne(other), attr, '')
                        , index=data.index, columns=data.columns)
'''
type_content = 1 -> notification: duplicate, missing dictionary,...
type_content = 2, 3 -> create audit tracking 
type_content = 4 -> check new key
'''
def create_content(type_content = None, type_sector = None, list_df = None, df_noti = None, engine =None):
    #Create content flat file
    if type_content == 1:
        df_noti_html = df_noti.to_html()
        return df_noti_html  
    
    elif type_content == 2:
        if type_sector == 1: #group and sum record
            df_flat_html = ''
            for df_flat in list_df:
                if len(df_flat) != 0:
                    for i in df_flat.columns:
                        if i == 'Sector':
                            sector = df_flat[i].iloc[0]
                            df_flat_html += '''<b><p style="font-size:85%; color: #736F6D">''' + sector + "</b></p>" + df_flat.to_html() + "<br>"
            return df_flat_html
                
        elif type_sector == 2: #group and count record
            df_flat_html = ''
            for df_flat in list_df:
                for i in df_flat.columns:
                    if i == 'Sector':
                        sector = df_flat[i].iloc[0]
                        df_flat_html += '''<b><p style="font-size:85%; color: #736F6D">''' + sector + "</b></p>" + df_flat.to_html() + "<br>"
            return df_flat_html
                
    #-------------------------------------------------------
    #'Create content DB base on df_flat_{}'.format(sector)
    elif type_content == 3:
        if type_sector == 1: #group and sum record
            list_df_query = []
            for df_flat in list_df:
                if len(df_flat) != 0:
                    df = tracking_db(df_flat, engine)
                    df=df.style.apply(highlight_diff, axis=None, other=df_flat) 
                    list_df_query.append(df)

            df_query_html = ''
            for df_query in list_df_query:
                for i in df_query.columns:
                    if i == 'Sector':
                        df_query_converted = df_query.data
                        sector = df_query_converted[i].iloc[0]
                        df_query_html += '''<b><p style="font-size:85%; color: #736F6D">''' + sector + "</b></p>" + df_query.to_html() + "<br>"
            return df_query_html
        
        elif type_sector == 2: #group and count record
            list_df_query = []
            for df_flat in list_df:
                df = tracking_db(df_flat, engine)
                df = df.style.apply(highlight_diff, axis=None, other=df_flat) 
                list_df_query.append(df)

            df_query_html = ''
            for df_query in list_df_query:
                for i in df_query.columns:
                    if i == 'Sector':
                        df_query_converted = df_query.data
                        sector = df_query_converted[i].iloc[0]
                        df_query_html += '''<b><p style="font-size:85%; color: #736F6D">''' + sector + "</b></p>" + df_query.to_html() + "<br>"
            return df_query_html
                
    elif type_content == 4:
        df_new_key_html = ''
        for df_flat in list_df:
            if len(df_flat) != 0:
                for i in df_flat.columns:
                    if i == 'Sector':
                        sector = df_flat[i].iloc[0]
                        df_new_key_html += '''<b><p style="font-size:85%; color: #736F6D">''' + sector + "</b></p>" + df_flat.to_html() + "<br>"
        return df_new_key_html
    
#------------------------------------------------------- 
'''
type_html = 1 -> notification: duplicate
type_html = 2 -> notification: missing dictionary
type_html = 3 -> create audit tracking 
type_html = 4 -> check new key
'''
def convert_df_to_html(type_html = None, df = None, list_df = None, type_sector = None, cnxn = None):
    if type_html == 1: #duplicate
        if type_sector == 1:
            list_columns_name = ['Sector', 'Project_City_Name', 'Date_Key', 'Sub_Project_Name']
            df_dup = df[list_columns_name]
            
        elif type_sector == 2:
            list_columns_name = ['City', 'Date_Key', 'Sub_Project_Name']
            df_dup = df[list_columns_name]
            df_dup.insert(loc=0, column='Sector', value='IP')
            
        df_dup = df_dup.reset_index(drop=True)
        df_noti_html = create_content(type_content = 1, df_noti = df_dup ,engine = cnxn)
        return df_noti_html
    
    elif type_html == 2: #dictionary
        df_dict = df.sort_values(by =['File_Name'],ascending=True).reset_index(drop=True)
        df_noti_html = create_content(type_content = 1, df_noti = df_dict ,engine = cnxn)
        return df_noti_html
    
    elif type_html == 3: # create tracking audit
        if type_sector == 1:
            df_flat_html = create_content(type_content = 2, type_sector = 1, list_df = list_df, engine = cnxn)
            df_query_html = create_content(type_content = 3, type_sector = 1, list_df = list_df, engine = cnxn)
            return df_flat_html, df_query_html
        
        elif type_sector == 2:
            df_flat_html = create_content(type_content = 2, type_sector = 2, list_df = list_df, engine = cnxn)
            df_query_html = create_content(type_content = 3, type_sector = 2, list_df = list_df, engine = cnxn)
            return df_flat_html, df_query_html    
        
    elif type_html == 4: #check new key
        df_new_key_html = create_content(type_content = 4, list_df = list_df, engine = cnxn)
        return df_new_key_html
    
#-------------------------------------------------------     
# Export list new key to confirm
'''def check_new_key(df_new_key = None, processed_data = None, sector = None):
    if sector in ['VLTH', 'RETAIL']:
        list_keys_on = ['Sector', 'Sub_Project_Name','Project_District_Name'
                        ,'Project_City_Name','Sub_Project_Type'
                       ]
        list_dim_columns = ['Sector', 'Project_City_Name', 'Date_Key', 'Project_District_Name'
                            , 'Project_Name', 'Sub_Project_Name', 'Sub_Project_Type', 'Project_Status', 'Developer', 'Launch_Year'
                           ]
        processed_data = processed_data.rename(columns = {'Project_Type':'Sub_Project_Type', 'Current_Status':'Project_Status'})
    elif sector in ['OFFICE','APT','APARTMENT','HOTEL','SA','SERVICED_APARTMENT']:
        list_keys_on = ['Sector', 'Sub_Project_Name','Project_District_Name'
                        , 'Project_City_Name','Grade'
                       ]
        list_dim_columns = ['Sector', 'Project_City_Name', 'Date_Key', 'Project_District_Name'
                            , 'Project_Name', 'Sub_Project_Name', 'Grade', 'Project_Status', 'Developer', 'Launch_Year'
                           ]
        processed_data = processed_data.rename(columns = {'Project_Grade':'Grade', 'Current_Status':'Project_Status'})
        
    elif sector == 'IP':
        list_keys_on = ['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type']
        list_dim_columns = ['City', 'Date_Key', 'District', 'Project_Name', 'Sub_Project_Name'
                            , 'Project_Type', 'Project_Sub_Type', 'Status', 'Developer_Name', 'Launch_Year'
                           ]

    if sector == 'RETAIL':
        df = processed_data[list_dim_columns]
        for i in list_keys_on:
            df = df[df[i].notnull()].copy()
            df.loc[:,i]=df[i].apply(lambda x: x.title())
        df['Sector']=df['Sector'].apply(lambda x: x.upper())
        df_new_key = pd.concat([df_new_key, df], axis=0)
        df_new_key=df_new_key.sort_values(by =['Project_City_Name', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
        return df_new_key

    elif sector == 'OFFICE':
        df = processed_data[list_dim_columns]
        for i in list_keys_on:
            df = df[df[i].notnull()].copy()
            df.loc[:,i]=df[i].apply(lambda x: x.title())
        df['Sector']=df['Sector'].apply(lambda x: x.upper())
        df_new_key = pd.concat([df_new_key, df], axis=0)
        df_new_key=df_new_key.sort_values(by =['Project_City_Name', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
        return df_new_key

    elif sector == 'HOTEL':
        df = processed_data[list_dim_columns]
        for i in list_keys_on:
            df = df[df[i].notnull()].copy()
            df.loc[:,i]=df[i].apply(lambda x: x.title())
        df['Sector']=df['Sector'].apply(lambda x: x.upper())
        df_new_key = pd.concat([df_new_key, df], axis=0)
        df_new_key=df_new_key.sort_values(by =['Project_City_Name', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
        return df_new_key

    elif sector == 'SA' or sector=='SERVICED_APARTMENT':
        df = processed_data[list_dim_columns]
        for i in list_keys_on:
            df = df[df[i].notnull()].copy()
            df.loc[:,i]=df[i].apply(lambda x: x.title())
        df['Sector']=df['Sector'].apply(lambda x: x.upper())
        df_new_key = pd.concat([df_new_key, df], axis=0)
        df_new_key=df_new_key.sort_values(by =['Project_City_Name', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
        return df_new_key

    elif sector == 'APT' or sector == 'APARTMENT':
        df = processed_data[list_dim_columns]
        for i in list_keys_on:
            df = df[df[i].notnull()].copy()
            df.loc[:,i]=df[i].apply(lambda x: x.title())
        df['Sector']=df['Sector'].apply(lambda x: x.upper())
        df_new_key = pd.concat([df_new_key, df], axis=0)
        df_new_key=df_new_key.sort_values(by =['Project_City_Name', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
        return df_new_key

    elif sector == 'VLTH':
        df = processed_data[list_dim_columns]
        for i in list_keys_on:
            df = df[df[i].notnull()].copy()
            df.loc[:,i]=df[i].apply(lambda x: x.title())
        df['Sector']=df['Sector'].apply(lambda x: x.upper())
        df_new_key = pd.concat([df_new_key, df], axis=0)
        df_new_key=df_new_key.sort_values(by =['Project_City_Name', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
        return df_new_key
    
    elif sector == 'IP':
        df = processed_data[list_dim_columns]
        for i in list_keys_on:
            df = df[df[i].notnull()].copy()
            df.loc[:,i]=df[i].apply(lambda x: x.title())
        df.insert(loc=0, column='Sector', value='IP')
        df_new_key = pd.concat([df_new_key, df], axis=0)
        for cols in df_new_key.columns:
            df_new_key[cols] = df_new_key[cols].fillna('NULL')
            df_new_key[cols] = df_new_key[cols].apply(lambda x: int(x) if isinstance(x, float) else x)
        df_new_key=df_new_key.sort_values(by =['City', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
        return df_new_key    
    
    else:
        return print(colored('Unknown sector','yellow'))'''

def check_new_key(df_new_key = None, processed_data = None, sector = None):
    if sector in ['VLTH', 'RETAIL']:
        list_keys_on = ['Sector', 'Sub_Project_Name','Project_District_Name'
                        ,'Project_City_Name','Sub_Project_Type'
                       ]
        list_dim_columns = ['Sector', 'Project_City_Name', 'Date_Key', 'Project_District_Name'
                            , 'Project_Name', 'Sub_Project_Name', 'Sub_Project_Type', 'Project_Status', 'Developer', 'Launch_Year'
                           ]
        processed_data = processed_data.rename(columns = {'Project_Type':'Sub_Project_Type', 'Current_Status':'Project_Status'})
    elif sector in ['OFFICE','APT','APARTMENT','HOTEL','SA','SERVICED_APARTMENT']:
        list_keys_on = ['Sector', 'Sub_Project_Name','Project_District_Name'
                        , 'Project_City_Name','Grade'
                       ]
        list_dim_columns = ['Sector', 'Project_City_Name', 'Date_Key', 'Project_District_Name'
                            , 'Project_Name', 'Sub_Project_Name', 'Grade', 'Project_Status', 'Developer', 'Launch_Year'
                           ]
        processed_data = processed_data.rename(columns = {'Project_Grade':'Grade', 'Current_Status':'Project_Status'})
        
    elif sector == 'IP':
        list_keys_on = ['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type']
        list_dim_columns = ['City', 'Date_Key', 'District', 'Project_Name', 'Sub_Project_Name'
                            , 'Project_Type', 'Project_Sub_Type', 'Status', 'Developer_Name', 'Launch_Year'
                           ]
    else:
        return print(colored('Unknown sector','yellow'))
    
    df = processed_data[list_dim_columns]
    if f'{sector}' == 'IP':
        df.insert(loc=0, column='Sector', value='IP')
    else:
        pass
    for i in list_keys_on:
        df = df[df[i].notnull()].copy()
        df.loc[:,i] = df[i].apply(lambda x: x.title())
        df.loc[:,'Sector'] = df.loc[:, 'Sector'].apply(lambda x: x.upper())
    df_new_key = pd.concat([df_new_key, df], axis=0)
    for cols in df_new_key.columns:
        df_new_key[cols] = df_new_key[cols].fillna('NULL')
        df_new_key[cols] = df_new_key[cols].apply(lambda x: int(x) if isinstance(x, float) else x)
    #df_new_key = df_new_key.reindex(columns = list_dim_columns)
    if f'{sector}' != 'IP':
        df_new_key = df_new_key.sort_values(by = ['Project_City_Name', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
    elif f'{sector}' == 'IP':
        df_new_key = df_new_key.sort_values(by = ['City', 'Launch_Year'],ascending=True, na_position='first', ignore_index=True)
    
    return df_new_key
    
    
#-------------------------------------------------------      
#Create audit table
def insert_to_tracking(list_df, table_name, cnxn):
    for df in list_df:
        if len(df) != 0:
            df['Import_Date'] = pd.to_datetime('today')
            df.to_sql(f'{table_name}', cnxn, index=False, if_exists='append', schema='Fresh')
    return print(colored('Inserted audit tracking sucessfully','green'))

#-------------------------------------------------------      
#Đổi Series ra dạng chũi và bỏ chữ viết hoa
def Convert_District(data,cnxn):
    Raw_dictrict=pd.read_sql('select * from GENERAL.District_Dictionary',cnxn)
    Raw_dictrict['Raw_District']=Raw_dictrict['Raw_District'].str.strip().str.lower()
    District=dict(zip(Raw_dictrict['Raw_District'],Raw_dictrict['Cleaned_District']))
    data['Project_District_Name']=data['Project_District_Name'].str.strip().str.lower()
    data['Project_District_Name']=data['Project_District_Name'].replace(District)
    data['Project_District_Name']=data['Project_District_Name'].str.title()
    return data    

#Check xem có giá trị null trong cột district không
def Check_District_Null(data):
    flag=0
    district_not_yet_entered=data['Project_Name'].iloc[(data.index[data['Project_District_Name'].isna()==True]).tolist()]
    if len(district_not_yet_entered)!=0:
        flag=1
    return flag,data,district_not_yet_entered



#Cờ 1: không tồn tại
#Cờ 0: có tồn tại
'''def Check_City(Sector, data, cnxn):
    list_economics_sector = ['MACRO', 'FDI', 'IP']
    if Sector in list_economics_sector:
        city = 'City'
    else:
        city = 'Project_City_Name'    
    flag_city=0
    Raw_city=pd.read_sql('select * from GENERAL.City_Dictionary',cnxn)
    Raw_city['Raw_City']=Raw_city['Raw_City'].str.strip().str.lower()
    data[city]=data[city].str.strip().str.lower()
    data['Convert_City'] = pd.merge(data, Raw_city, how='left', 
                                    left_on=city, 
                                    right_on='Raw_City')['Cleaned_City']
    city_not_in_dict = data[city][data['Convert_City'].isnull()]
    if len(city_not_in_dict) != 0:
        flag_city=1
        return flag_city,data,city_not_in_dict
    else:
        data[city] = data['Convert_City']
        data = data.drop(columns=['Convert_City'])
        return flag_city,data,city_not_in_dict

    
def Check_District(Sector, data, cnxn):
    if Sector =='MACRO' or Sector =='FDI' or Sector =='IP':
        district = 'District'
    else:
        district = 'Project_District_Name'
    flag_district=0
    Raw_dictrict=pd.read_sql('select * from GENERAL.District_Dictionary',cnxn)
    Raw_dictrict['Raw_District']=Raw_dictrict['Raw_District'].str.strip().str.lower()
    data[district]=data[district].str.strip().str.lower()
    data['Convert_District'] = pd.merge(data, Raw_dictrict, 
                                        how='left', 
                                        left_on=district, 
                                        right_on='Raw_District')['Cleaned_District']
    district_not_in_dict = data[district][data['Convert_District'].isnull()]
    if len(district_not_in_dict) != 0:
        flag_district=1
        return flag_district,data,district_not_in_dict
    else:
        data[district] = data['Convert_District']
        data = data.drop(columns=['Convert_District'])
        return flag_district,data,district_not_in_dict


def Check_Status(data,cnxn):
    flag_status=0
    Raw_status=pd.read_sql('select * from [GENERAL].[Status_Dictionary]',cnxn)
    Raw_status['Raw_Status']=Raw_status['Raw_Status'].str.strip().str.lower()
    data['Project_Status']=data['Project_Status'].str.strip().str.lower()
    data['Convert_Status'] = pd.merge(data, Raw_status, how='left', 
                                      left_on='Project_Status', 
                                      right_on='Raw_Status')['Cleaned_Status']
    status_not_in_dict = data['Project_Status'][data['Convert_Status'].isnull()]
    if len(status_not_in_dict) != 0:
        flag_status=1
        return flag_status,data,status_not_in_dict
    else:
        data['Project_Status'] = data['Convert_Status']
        data = data.drop(columns=['Convert_Status'])
        return flag_status,data,status_not_in_dict
    
    
def Check_Type(data,cnxn,Sector):
    if Sector=='RETAIL' or Sector=='VLTH' or Sector == 'APARTMENT' or Sector == 'APT':
        Raw_Type=pd.read_sql("select * from GENERAL.Project_Type_Dictionary WHERE Sector = '{}' ".format(Sector),cnxn)
        flag_type=0
        Raw_Type['Raw_Type']=Raw_Type['Raw_Type'].str.strip().str.lower()
        data['Sub_Project_Type']=data['Sub_Project_Type'].str.strip().str.lower()
        data['Sub_Project_Type']=data['Sub_Project_Type'].replace(np.nan,None)
        data['Convert_Type'] = pd.merge(data, Raw_Type, 
                                        how='left', 
                                        left_on=['Sector', 'Sub_Project_Type'], 
                                        right_on=['Sector', 'Raw_Type'])['Cleaned_Type']
        Type_not_in_dict = data['Sub_Project_Type'][data['Convert_Type'].isnull()]
        if len(Type_not_in_dict) != 0:
            flag_type=1
            return flag_type,data,Type_not_in_dict
        else:
            data['Sub_Project_Type']= data['Convert_Type']
            data = data.drop(columns=['Convert_Type'])
            return flag_type,data,Type_not_in_dict
    else:
        return 0,data,0
    
    
def Check_Grade(data,cnxn,Sector):
    if Sector == 'HOTEL' or Sector == 'OFFICE' or Sector == 'APARTMENT' or Sector == 'APT' or Sector == 'SERVICED_APARTMENT' or Sector=='SA':
        Raw_Grade=pd.read_sql('select * from [GENERAL].[Project_Grade_Dictionary]',cnxn)
        flag_grade=0
        Raw_Grade['Raw_Grade']=Raw_Grade['Raw_Grade'].str.strip().str.lower()
        data['Grade']=data['Grade'].str.strip().str.lower()
        data['Convert_Grade'] = pd.merge(data, Raw_Grade, 
                                         how='left', 
                                         left_on='Grade', 
                                         right_on='Raw_Grade')['Cleaned_Grade']
        Type_not_in_dict = data['Grade'][data['Convert_Grade'].isnull()]
        if len(Type_not_in_dict) != 0:
            flag_grade=1
            return flag_grade,data,Type_not_in_dict
        else:
            data['Grade'] = data['Convert_Grade']
            data = data.drop(columns=['Convert_Grade'])
            return flag_grade,data,Type_not_in_dict
    else:
        return 0,data,0'''