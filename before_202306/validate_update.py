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
from validate_update import *
from unidecode import unidecode
from termcolor import colored

#get data
def get_data(relative_url, file_url):
    header_BIHub = 'share_point_BIHub'
    config_BIHub = read_config_json(config_path, header_BIHub)
    BIHub = SharePoint(config_BIHub)
    
    sp_object = relative_url.split('/')[2].replace('-','')
    data = eval(sp_object).get_file(file_url)   
    #Remove irrelevant data
    # data.dropna(axis=0, how='all', thresh=None, subset=None, inplace=True)#remove blank rows   
    data.dropna(axis = 0, how = 'all', inplace = True)#remove blank rows
    data.drop(data.columns[data.columns.str.contains('Unnamed')], axis=1, inplace=True)#remove blank columns
    data.columns = data.columns.str.strip().str.replace(r'\s+', '', regex=True)#strip column
    file_name = file_url.split('/')[-1].split('.')[0]
    sector = file_url.split('/')[-1].split('_')[1].upper()
    if sector=='APT':
        sector='APARTMENT'
    elif sector=='SA':
        sector='SERVICED_APARTMENT'
    else:
        pass
    return data, file_name, sector


               
def remove_unformated_character(data):
    list_unformated_character = ['#N/a', 'N/A', 'N/a', 'n/a', 'NA', '-'
                                 , '$-', '#DIV/0!', 'FALSE'   
                                ]
    list_keys_on = ['Sector', 'Sub_Project_Name', 'Project_District_Name', 'Project_City_Name'
                    , 'Project_Type', 'Project_Grade'
                    , 'District', 'City', 'Project_Sub_Type'
                   ]
    for columns in data.columns:
        data[columns] = data[columns].replace(r'^\s*$', np.nan, regex=True)#Replacing blank values (has white space)
        data[columns] = data[columns].replace('(^\s+|\s+$)', '', regex=True)
        data[columns] = data[columns].replace(r'\s+', ' ', regex=True)#remove line breaks from an excel cells
        data[columns] = data[columns].replace('\.0$','', regex=True)
        if type(columns) == str:
            data[columns] = convert_str(data[columns])
            if columns not in list_keys_on:
                data[columns] = data[columns].replace(',','', regex=True)
            data.loc[:,columns] = data[columns].apply(lambda x: x.title() if isinstance(x, str) else x)
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
    if parameter in ['City', 'District', 'Status', 'Indicator']:
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
                      , 'APT', 'IP'
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
#     data[f'Convert_{parameter}'] = pd.merge(data, raw_parameter
#                                             , how='left'
#                                             , left_on=f'{column_name}'
#                                             , right_on=f'Raw_{parameter}')[f'Cleaned_{parameter}']

    # Hieu update - 31-05-2023
    get_cleaned_type = lambda x: raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x, f'Cleaned_{parameter}'].values[0] if len(raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x, f'Cleaned_{parameter}']) > 0 else None
    # get_cleaned_type = lambda x: raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x,f'Cleaned_{parameter}'].values[0] #fix lỗi duplicate của hàm merge trên
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
    '''if x!=None:
        x=str(x).replace(',','')'''
    try:
        return float(x)
    except:
        return None
    
    
def convert_int(x):
    try:
        return int(x)
    except:
        return None
    
    
def convert_str(df_data):
    df_data = df_data.fillna('NULL')
    df_data = df_data.astype(str)
    df_data = df_data.replace('NULL',np.nan)
    return df_data.replace('\.0$','', regex=True)


def Generate_Additional_Columns(data,list_file,df_summ_file,BIHub,engine,file_url):
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
###Check key

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
        
    elif sector == 'IP':
        list_keys=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type', 'Project_Key']
        list_keys_on=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type']

    if sector != 'IP':
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
def get_project_key(flag_key, processed_data, data, sector, engine):
    if sector in ['VLTH', 'RETAIL']:
        list_keys = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Type','Project_Key']
        list_keys_on =['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Type']

    elif sector in ['OFFICE','APT','APARTMENT','HOTEL','SA','SERVICED_APARTMENT']:
        list_keys=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys_on=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade']
        
    elif sector == 'IP':
        list_keys=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type', 'Project_Key']
        list_keys_on=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type']
        
    if sector != 'IP':
        table_name = 'Main_Sector_Project'
    else:
        table_name = 'Industrial_Park_Project'
    #-------------------------------------------------------  
    #Flag 0: key has existed
    #Flag 1: key hasn't existed
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x   
    #create new key
    if flag_key==1:
        print(colored('Cant get key', 'yellow'))
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
        else:
            data.rename(columns = {'Project_Key_y':'Project_Key'}, inplace = True)
            #data['File_Date'] = pd.to_datetime(data['File_Date']).dt.date
        return data
        
    else:
        print(colored('Unknown sector in get key section','yellow'))
        return data

#-------------------------------------------------------
#get flat file to update
def convert_list_to_tupple(data, column_name):
    column_name = list(set(data[f'{column_name}'].tolist()))
    column_name = f"({str(f'{column_name}')[1:-1]})"
    return column_name

def get_flat_file(data, sector, engine):
    if sector != 'IP':
        if sector=='APARTMENT':
            sector='APT'
        list_columns = ['Project_City_Name', 'Date_Key']
        for i in list_columns:
            globals()[f'list_{i.lower()}'] = convert_list_to_tupple(data, i)
        df_query = pd.read_sql('''
                        WITH temp_update AS
                        (
                            SELECT t.*,
                                    DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY Import_Date DESC) Rn
                            FROM FRESH.{} t
                        )
                            SELECT *
                            FROM temp_update t
                            WHERE t.Rn = 1
                            AND t.Project_City_Name IN {}
                            AND t.Date_Key IN {}
                             '''.format(sector, list_project_city_name, list_date_key),engine)
    elif sector == 'IP':
        sector = 'Industrial_Park'
        list_columns = ['City', 'Date_Key']
        for i in list_columns:
            globals()[f'list_{i.lower()}'] = convert_list_to_tupple(data, i)
        df_query = pd.read_sql('''
                        WITH temp_update AS
                        (
                            SELECT t.*,
                                    DENSE_RANK() OVER(PARTITION BY t.File_Name ORDER BY Import_Date DESC) Rn
                            FROM FRESH.{} t
                        )
                            SELECT *
                            FROM temp_update t
                            WHERE t.Rn = 1
                            AND t.City IN {}
                            AND t.Date_Key IN {}
                             '''.format(sector, list_city, list_date_key),engine)
    if 'Import_date' in df_query.columns:
        df_query = df_query.rename(columns={'Import_date':'Import_Date'})
    else:
        pass
    list_drop_columns = ['Surrogate_ID', 'Import_Date', 'Rn']
    df_query = df_query.drop(columns = list_drop_columns, axis = 1)
    return df_query

#update step
"""def update_flat_file(sector, df_flat, df_update):
    if 'Project_key' in df_flat.columns:
        df_flat = df_flat.rename(columns={'Project_key':'Project_Key'})
    else:
        pass
    list_keys_on = ['Project_Key', 'Date_Key']
    if sector == 'RETAIL':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'GFA', 'NLA', 'Vaccant_Area', 'Leased_Area'
                       , 'Avg_Gross_Rent', 'New_Supply'
                      ]
    elif sector == 'OFFICE':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'NLA', 'Leased_Area_End_Of_Q', 'Vacant_Area'
                       , 'Avg_Net_Rent', 'Avg_Gross_Rent', 'New_Supply'
                      ]
    elif sector == 'HOTEL':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'Total_Supply', 'Leased_Rooms', 'Avg_Room_Price'
                       , 'Quoted_Room_Rate', 'Occupancy', 'New_Supply'
                       ]
    elif sector == 'SERVICED_APARTMENT' or sector == 'SA':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'Total_Supply', 'Launch_Units', 'Leased_Units', 'Vacant_Units'
                       , 'Avg_Rent', 'Net_Lettable', 'Rev_PAU', 'Rev_PAU_m2', 'Rent_Per_Unit', 'New_Supply'
                       ]
    elif sector == 'APT' or sector=='APARTMENT':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'Total_Supply', 'Secondary_Supply', 'Newly_Launch', 'Total_Launched_Units'
                       , 'Available_Units', 'Total_Sold_Units','Sold_Units_End_Of_Q', 'Quarterly_Sales'
                       , 'Remaining_Supply', 'NFA_Primary_Price', 'NSA_Primary_Price', 'UNIT_Primary_Price'
                       , 'Avg_Primary_Price', 'NFA_Secondary_Price', 'NSA_Secondary_Price', 'UNIT_Secondary_Price'
                       , 'Avg_Secondary_Price', 'Quarterly_Sales_New_Supply'
                       ]
    elif sector == 'VLTH':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'Total_Supply', 'Newly_Launch', 'Total_Launched_Units', 'Available_Units'
                       , 'Total_Sold_Units', 'Sold_Units_End_Of_Q', 'Quarterly_Sales', 'Remaining_Supply'
                       , 'LA_Primary_Price', 'GFA_Primary_Price', 'UNIT_Primary_Price', 'LA_Secondary_Price'
                       , 'GFA_Secondary_Price', 'UNIT_Secondary_Price', 'Quarterly_Sales_New_Supply'
                       ]
    elif sector == 'IP':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Status'
                       , 'PROJECT_Land_Area', 'LAND_Leaseable_Land_Area', 'LAND_Available_Land_Area', 'LAND_Leased_Land_Area'
                       , 'RB_Land_Area', 'RB_Total_GFA', 'RB_NLA', 'RB_Leased_Area', 'Occupancy'
                       , 'LAND_AVG_Rent_Primary_Price', 'LAND_AVG_Rent_Secondary_Price', 'LAND_AVG_Rent_Actual'
                       , 'LAND_AVG_Rent_Last_Transaction', 'RB_AVG_Rent_Primary_Price', 'RB_AVG_Rent_Secondary_Price'
                       , 'RB_AVG_Rent_Actual', 'RB_AVG_Rent_Last_Transaction'
                      ]
    
    df_merge = pd.merge(df_flat, df_update[list_update], 
                                 left_on = list_keys_on,
                                 right_on = list_keys_on,
                                 how = 'left'
                                )
    for i in list_update:
        if i not in list_keys_on:
            df_merge[f'{i}'] = df_merge[f'{i}_y'].fillna(df_merge[f'{i}_x'])
    for i in df_merge[list(df_merge.columns)]:
        df_merge[i] = df_merge[i].replace({np.nan: None})
    df_merge = df_merge.drop([x for x in df_merge if x.endswith(('_x', '_y'))], axis = 1)
    df_merge['Import_Date'] = pd.to_datetime('today')
    
    return df_merge"""


def update_flat_file(sector, df_flat, df_update):
    flag_update = 0
    if 'Project_key' in df_flat.columns:
        df_flat = df_flat.rename(columns={'Project_key':'Project_Key'})
    else:
        pass
    list_keys_on = ['Project_Key', 'Date_Key']
    if sector == 'RETAIL':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'GFA', 'NLA', 'Vacant_Area', 'Leased_Area'
                       , 'Avg_Gross_Rent', 'New_Supply'
                      ]
    elif sector == 'OFFICE':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'NLA', 'Leased_Area_End_Of_Q', 'Vacant_Area'
                       , 'Avg_Net_Rent', 'Avg_Gross_Rent', 'New_Supply'
                      ]
    elif sector == 'HOTEL':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'Total_Supply', 'Leased_Rooms', 'Avg_Room_Price'
                       , 'Quoted_Room_Rate', 'Occupancy', 'New_Supply'
                       ]
    elif sector == 'SERVICED_APARTMENT' or sector == 'SA':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'Total_Supply', 'Launch_Units', 'Leased_Units', 'Vacant_Units'
                       , 'Avg_Rent', 'Net_Lettable', 'Rev_PAU', 'Rev_PAU_m2', 'Rent_Per_Unit', 'New_Supply'
                       ]
    elif sector == 'APT' or sector=='APARTMENT':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'Total_Supply', 'Secondary_Supply', 'Newly_Launch', 'Total_Launched_Units'
                       , 'Available_Units', 'Total_Sold_Units','Sold_Units_End_Of_Q', 'Quarterly_Sales'
                       , 'Remaining_Supply', 'NFA_Primary_Price', 'NSA_Primary_Price', 'UNIT_Primary_Price'
                       , 'Avg_Primary_Price', 'NFA_Secondary_Price', 'NSA_Secondary_Price', 'UNIT_Secondary_Price'
                       , 'Avg_Secondary_Price', 'Quarterly_Sales_New_Supply'
                       ]
    elif sector == 'VLTH':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Project_Status'
                       , 'Total_Supply', 'Newly_Launch', 'Total_Launched_Units', 'Available_Units'
                       , 'Total_Sold_Units', 'Sold_Units_End_Of_Q', 'Quarterly_Sales', 'Remaining_Supply'
                       , 'LA_Primary_Price', 'GFA_Primary_Price', 'UNIT_Primary_Price', 'LA_Secondary_Price'
                       , 'GFA_Secondary_Price', 'UNIT_Secondary_Price', 'Quarterly_Sales_New_Supply'
                       ]
    elif sector == 'IP':
        list_update = ['Project_Key', 'Date_Key', 'Form', 'Status'
                       , 'PROJECT_Land_Area', 'LAND_Leaseable_Land_Area', 'LAND_Available_Land_Area', 'LAND_Leased_Land_Area'
                       , 'RB_Land_Area', 'RB_Total_GFA', 'RB_NLA', 'RB_Leased_Area', 'Occupancy'
                       , 'LAND_AVG_Rent_Primary_Price', 'LAND_AVG_Rent_Secondary_Price', 'LAND_AVG_Rent_Actual'
                       , 'LAND_AVG_Rent_Last_Transaction', 'RB_AVG_Rent_Primary_Price', 'RB_AVG_Rent_Secondary_Price'
                       , 'RB_AVG_Rent_Actual', 'RB_AVG_Rent_Last_Transaction'
                      ]
    #------------------------------------------------------- 
    int_function = lambda x: int(x) if isinstance(x, str) else x
    df_update[list_keys_on] = df_update[list_keys_on].applymap(int_function)
    df_flat[list_keys_on] = df_flat[list_keys_on].applymap(int_function)
    merge_how = ['left', 'inner']    
    for i in merge_how:
        globals()[f'df_merge_{i.lower()}'] = pd.merge(df_update, df_flat[list_update]
                             , left_on = list_keys_on
                             , right_on = list_keys_on
                             , how = i
                            )
    #check project_key có thật sự thuộc về flat file đó không
    if len(df_merge_inner) < len(df_merge_left):
        flag_update = 1
        df_merge_gap = df_merge_left[~df_merge_left['Project_Key'].isin(df_merge_inner['Project_Key'])]
        return flag_update, df_merge_gap
    else:
        df_merge = pd.merge(df_flat, df_update[list_update], 
                                 left_on = list_keys_on,
                                 right_on = list_keys_on,
                                 how = 'left'
                                )
        for i in list_update:
            if i not in list_keys_on:
                df_merge[f'{i}'] = df_merge[f'{i}_y'].fillna(df_merge[f'{i}_x'])
        for i in df_merge[list(df_merge.columns)]:
            df_merge[i] = df_merge[i].replace({np.nan: None})
        df_merge = df_merge.drop([x for x in df_merge if x.endswith(('_x', '_y'))], axis = 1)
        df_merge['Import_Date'] = pd.to_datetime('today')

        return flag_update, df_merge
#-------------------------------------------------------
###Insert to DB            
def insert_to_fresh(file_url, data, cnt_str):
#     conn ='Driver={ODBC Driver 17 for SQL Server};Server=tcp:hkazdevsqld3vnreserch.database.windows.net,1433;Database=D3VNResearch_Staging;Uid=D3VNResearch@savills.com.vn;Pwd=Wy6bF!4ncy6U+vxN;Encrypt=yes;Authentication=ActiveDirectoryPassword;Connection Timeout=30;'
#     quoted = quote_plus(conn)
#     engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

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
    sector = file_url.split('/')[-1].split('_')[1].upper()
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
#-------------------------------------------------------      
#Create audit table
def insert_to_tracking(df_update, sector, cnxn):
    if sector == 'IP':
        sector = 'Industrial_Park'
    df_update.to_sql(f'Tracking_Update_{sector.title()}', cnxn, index=False, if_exists='append', schema='FRESH')

#-------------------------------------------------------
def check_new_key(file_url, data, sector, engine):
    flag_new_key=0
    if 'New_Project_Key' not in data.columns:
        data['New_Project_Key'] = np.nan
        data['New_Project_Key'] = data['New_Project_Key'].replace({np.nan: None})
    else:
        pass
    if sector in ['VLTH', 'RETAIL']:
        list_rename = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Type','Project_Key']
        list_keys = ['Sector', 'New_Sub_Project_Name', 'New_Project_District_Name','New_Project_City_Name','New_Project_Type','New_Project_Key']
        list_keys_on =['Sector', 'New_Sub_Project_Name','New_Project_District_Name','New_Project_City_Name','New_Project_Type']
        data = data.rename(columns = {'New_Sub_Project_Type':'New_Project_Type'})
        
    elif sector in ['OFFICE','APT','APARTMENT', 'HOTEL','SA','SERVICED_APARTMENT']:
        list_rename=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys = ['Sector', 'New_Sub_Project_Name', 'New_Project_District_Name','New_Project_City_Name','New_Project_Grade','New_Project_Key']
        list_keys_on=['Sector', 'New_Sub_Project_Name','New_Project_District_Name','New_Project_City_Name','New_Project_Grade']
        data = data.rename(columns = {'New_Grade':'New_Project_Grade'})
        
    elif sector == 'IP':
        list_rename=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type', 'Project_Key']
        list_keys=['New_Sub_Project_Name', 'New_District', 'New_City', 'New_Project_Sub_Type', 'New_Project_Key']
        list_keys_on=['Sub_Project_Name', 'New_District', 'New_City', 'New_Project_Sub_Type']
        
    #Flag 0: key has existed
    #Flag 1: key hasn't existed
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x
    if sector != 'IP':
        df_query = pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
    elif sector =='IP':
        df_query = pd.read_sql("select * from FRESH.Industrial_Park_Project",engine)
    for i in df_query[list_rename]:
        if i != 'Sector':
            df_query = df_query.rename(columns = {i:f'New_{i}'})
    data[list_keys_on] = data[list_keys_on].applymap(lower_function)
    df_query[list_keys] = df_query[list_keys].applymap(lower_function)
    merged_data=pd.merge(data, df_query[list_keys],
                         left_on = list_keys_on,
                         right_on = list_keys_on,
                         how = 'left'
                        )
    new_processed_data=merged_data[merged_data['New_Project_Key_y'].isnull()]
        
    for i in list_keys_on:
            data[i]=data[i].str.title()
    if new_processed_data.count().sum() != 0:
        flag_new_key=1
        return new_processed_data, flag_new_key
    
    elif new_processed_data.count().sum() == 0:
        return new_processed_data, flag_new_key
    
    else:
        print(colored('Unknown sector in check project key section','yellow'))
        return new_processed_data, flag_new_key

#-------------------------------------------------------
def get_new_key(flag_new_key, new_processed_data, data, sector, engine):
    if sector in ['VLTH', 'RETAIL']:
        list_rename = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Type','Project_Key']
        list_keys = ['Sector', 'New_Sub_Project_Name', 'New_Project_District_Name','New_Project_City_Name','New_Project_Type','New_Project_Key']
        list_keys_on =['Sector', 'New_Sub_Project_Name','New_Project_District_Name','New_Project_City_Name','New_Project_Type']
        data = data.rename(columns = {'New_Sub_Project_Type':'New_Project_Type'})
        
    elif sector in ['OFFICE','APT','APARTMENT', 'HOTEL','SA','SERVICED_APARTMENT']:
        list_rename=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys = ['Sector', 'New_Sub_Project_Name', 'New_Project_District_Name','New_Project_City_Name','New_Project_Grade','New_Project_Key']
        list_keys_on=['Sector', 'New_Sub_Project_Name','New_Project_District_Name','New_Project_City_Name','New_Project_Grade']
        data = data.rename(columns = {'New_Grade':'New_Project_Grade'})
        
    elif sector == 'IP':
        list_rename=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type', 'Project_Key']
        list_keys=['New_Sub_Project_Name', 'New_District', 'New_City', 'New_Project_Sub_Type', 'New_Project_Key']
        list_keys_on=['Sub_Project_Name', 'New_District', 'New_City', 'New_Project_Sub_Type']
        
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x       
    if flag_new_key==1:#Create new key
        print(colored('Cant get new key', 'yellow'))
        return data
            
    elif flag_new_key==0:
        if sector != 'IP':
            df_query = pd.read_sql("select * from FRESH.Main_Sector_Project",engine)
        elif sector =='IP':
            df_query = pd.read_sql("select * from FRESH.Industrial_Park_Project",engine)
        for i in df_query[list_rename]:
            if i != 'Sector':
                df_query = df_query.rename(columns = {i:f'New_{i}'})
        data[list_keys_on] = data[list_keys_on].applymap(lower_function)
        df_query[list_keys] = df_query[list_keys].applymap(lower_function)
        data=pd.merge(data, df_query[list_keys],
                             left_on = list_keys_on,
                             right_on = list_keys_on,
                             how = 'left'
                            )
        data.drop(columns='New_Project_Key_x', axis=1,inplace=True)
        for i in list_keys_on:
            data[i]=data[i].str.title()
        if sector in ['VLTH', 'RETAIL']:
            data['Sector']=data['Sector'].str.upper()
            data.rename(columns = {'New_Project_Key_y':'New_Project_Key','New_Project_Type':'New_Sub_Project_Type'}, inplace = True)
        elif sector in ['OFFICE','APT','APARTMENT', 'HOTEL','SA','SERVICED_APARTMENT']:
            data['Sector']=data['Sector'].str.upper()
            data.rename(columns = {'New_Project_Key_y':'New_Project_Key','New_Project_Grade':'New_Grade'}, inplace = True)
        elif sector == 'IP':
            data.rename(columns = {'New_Project_Key_y':'New_Project_Key'}, inplace = True)
            
        return data
      
#-------------------------------------------------------    