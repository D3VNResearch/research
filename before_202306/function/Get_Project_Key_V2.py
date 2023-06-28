import pandas as pd
import numpy as np
import csv
import pyodbc
import glob
import os
import datetime
import pytz
import re
import unicodedata
from function.Insert_Fresh import *
convert=lambda x: str(x).replace(',','').replace(' -   ','') if  x!=np.nan or x not in ('  ') else x

def Convert_Sub_Type(data,cnxn):
    Raw_Type=pd.read_sql('select * from GENERAL.Project_Type_Dictionary',cnxn)
    Raw_Type['Raw_Type']=Raw_Type['Raw_Type'].str.strip().str.lower()
    District=dict(zip(Raw_Type['Raw_Type'],Raw_Type['Cleaned_Type']))
    data['Sub_Project_Type']=data['Sub_Project_Type'].str.strip().str.lower()
    data['Sub_Project_Type']=data['Sub_Project_Type'].replace(District)
    data['Sub_Project_Type']=data['Sub_Project_Type'].str.title()
    return data
def Project_key(City,Sector,cnxn):
    if Sector=='VLTH' or Sector=='RETAIL':
        project_key=pd.read_sql("Select max(Project_Key) as 'PROJECT_KEY',Sub_Project_Name,Project_Type,Project_District_Name from FRESH.Main_Sector_Project where (Project_City_Name like'%s' and Sector like'%s') GROUP BY Sub_Project_Name, Project_Type, Project_District_Name"%(City,Sector),cnxn)
        project_key['Project_Type']=project_key['Project_Type'].str.strip()
    else:
        project_key=pd.read_sql("Select max(Project_Key) as 'PROJECT_KEY',Sub_Project_Name,Project_Grade,Project_District_Name from FRESH.Main_Sector_Project where (Project_City_Name like'%s' and Sector like'%s') GROUP BY Sub_Project_Name ,Project_Grade ,Project_District_Name "%(City,Sector),cnxn)
    return project_key
def Merge(Sector,data,project_key):
    #Vì trong pandas phân biệt chữ hoa với thường
    data['Sub_Project_Name']=data['Sub_Project_Name'].str.lower().str.strip()
    data['Project_District_Name']=data['Project_District_Name'].str.lower().str.strip()
    project_key['Sub_Project_Name']=project_key['Sub_Project_Name'].str.lower().str.strip()
    project_key['Project_District_Name']=project_key['Project_District_Name'].str.lower().str.strip()
    if Sector=='VLTH' or Sector=='RETAIL':
        project_key['Project_Type']=project_key['Project_Type'].str.lower().str.strip()
        data['Sub_Project_Type']=data['Sub_Project_Type'].str.lower().str.strip()
        Merge_data=data.merge(project_key, left_on=['Sub_Project_Name' , 'Sub_Project_Type','Project_District_Name'], right_on=['Sub_Project_Name','Project_Type','Project_District_Name'], how='left')
        #Import vô DB với chữ hoa
        Merge_data['Sub_Project_Name']=Merge_data['Sub_Project_Name'].str.title()
        Merge_data['Project_Type']=Merge_data['Project_Type'].str.title()
        Merge_data['Project_District_Name']=Merge_data['Project_District_Name'].str.title()
        Merge_data['Sub_Project_Type']=Merge_data['Sub_Project_Type'].str.title()
        project_key['Project_Type']=project_key['Project_Type'].str.title()
        data['Sub_Project_Type']=data['Sub_Project_Type'].str.title()
    else:
        data['Grade']=data['Grade'].str.lower().str.strip()
        project_key['Project_Grade']=project_key['Project_Grade'].str.lower().str.strip()
        Merge_data=data.merge(project_key, left_on=['Sub_Project_Name' ,'Grade', 'Project_District_Name'],right_on=['Sub_Project_Name','Project_Grade','Project_District_Name'], how='left')
        Merge_data['Sub_Project_Name']=Merge_data['Sub_Project_Name'].str.title()
        Merge_data['Grade']=Merge_data['Grade'].str.title()
        Merge_data['Project_Grade']=Merge_data['Project_Grade'].str.title()
        data['Grade']=data['Grade'].str.title()
        project_key['Project_Grade']=project_key['Project_Grade'].str.title()
        Merge_data['Project_District_Name']=Merge_data['Project_District_Name'].str.title()
    data['Project_District_Name']=data['Project_District_Name'].str.title()
    project_key['Project_District_Name']=project_key['Project_District_Name'].str.title()
    data['Sub_Project_Name']=data['Sub_Project_Name'].str.title()
    project_key['Sub_Project_Name']=project_key['Sub_Project_Name'].str.title()
    return Merge_data
def Insert_DB(Sector,empty,cnxn,City,File_Date):
    Import_Date=pd.to_datetime('today')
    cursor=cnxn.cursor()
    #empty['project.SUB_PROJECT_TYPE']=empty['project.SUB_PROJECT_TYPE'].str.strip()
    if 'LAT' and 'LONG' in empty.columns:
        if Sector=='OFFICE':
            for index, row in empty.iterrows():
                cursor.execute("INSERT INTO APP.D_PROJECT(PROJECT_NAME_DOC,SUB_PROJECT_CODE,SUB_PROJECT_TYPE,LAUNCHING_TIME,CURRENT_GRADE,CURRENT_STATUS,LATITUDE,LONGITUDE,W_IS_DELETED,CITY_NAME_DOC,DISTRICT_NAME_DOC,DEVELOPER_NAME_DOC,OPERATOR_NAME_DOC,CONSTRUCTOR_NAME_DOC,W_INSERT_DT,SECTOR) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row['project.PROJECT_NAME_DOC'],row['project.ORIGINAL_SUB_PROJECT_NAME_DOC'],row['project.SUB_PROJECT_TYPE'],row['project.LAUNCHING_TIME'],row['project.CURRENT_GRADE'],row['office.PROJECT_STATUS'],row['LAT'],row['LONG'],'0',City,row['location.DISTRICT_NAME_DOC'],row['developer.MAIN_DEVELOPER_NAME_DOC'],row['operator.MAIN_OPERATOR_NAME_DOC'],row['constructor.MAIN_CONSTRUCTOR_NAME_DOC'],today,'OFFICE')
        elif Sector=='APARTMENT':
           
            for index, row in empty.iterrows():
                print(row)
                cursor.execute("INSERT INTO APP.D_PROJECT(PROJECT_NAME_DOC,SUB_PROJECT_CODE,SUB_PROJECT_TYPE,LAUNCHING_TIME,CURRENT_GRADE,CURRENT_STATUS,LATITUDE,LONGITUDE,W_IS_DELETED,CITY_NAME_DOC,DISTRICT_NAME_DOC,DEVELOPER_NAME_DOC,OPERATOR_NAME_DOC,CONSTRUCTOR_NAME_DOC,W_INSERT_DT,SECTOR,PROFIT_SHARING, TENURE, HANDOVER_CONDITION) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row['project.PROJECT_NAME_DOC'],row['project.ORIGINAL_SUB_PROJECT_NAME_DOC'],row['project.SUB_PROJECT_TYPE'],row['project.LAUNCHING_TIME'],row['project.CURRENT_GRADE'],row['apartment.PROJECT_STATUS'],row['LAT'],row['LONG'],'0',City,row['location.DISTRICT_NAME_DOC'],row['developer.MAIN_DEVELOPER_NAME_DOC'],row['operator.MAIN_OPERATOR_NAME_DOC'],row['constructor.MAIN_CONSTRUCTOR_NAME_DOC'],today,'APARTMENT',row['apartment.PROFIT_SHARING'],row['apartment.TENURE'],row['apartment.HANDOVER_CONDITION'])
        elif Sector=='HOTEL':
            for index, row in empty.iterrows():
                cursor.execute("INSERT INTO APP.D_PROJECT(PROJECT_NAME_DOC,SUB_PROJECT_CODE,SUB_PROJECT_TYPE,LAUNCHING_TIME,CURRENT_GRADE,CURRENT_STATUS,LATITUDE,LONGITUDE,W_IS_DELETED,CITY_NAME_DOC,DISTRICT_NAME_DOC,DEVELOPER_NAME_DOC,OPERATOR_NAME_DOC,CONSTRUCTOR_NAME_DOC,W_INSERT_DT,SECTOR) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row['project.PROJECT_NAME_DOC'],row['project.ORIGINAL_SUB_PROJECT_NAME_DOC'],row['project.SUB_PROJECT_TYPE'],row['project.LAUNCHING_TIME'],row['project.CURRENT_GRADE'],row['hotel.PROJECT_STATUS'],row['LAT'],row['LONG'],'0',City,row['location.DISTRICT_NAME_DOC'],row['developer.MAIN_DEVELOPER_NAME_DOC'],row['operator.MAIN_OPERATOR_NAME_DOC'],row['constructor.MAIN_CONSTRUCTOR_NAME_DOC'],today,'HOTEL')
        elif Sector=='SERVICED_APARTMENT':
            for index, row in empty.iterrows():
                cursor.execute("INSERT INTO APP.D_PROJECT(PROJECT_NAME_DOC,SUB_PROJECT_CODE,SUB_PROJECT_TYPE,LAUNCHING_TIME,CURRENT_GRADE,CURRENT_STATUS,LATITUDE,LONGITUDE,W_IS_DELETED,CITY_NAME_DOC,DISTRICT_NAME_DOC,DEVELOPER_NAME_DOC,OPERATOR_NAME_DOC,CONSTRUCTOR_NAME_DOC,W_INSERT_DT,SECTOR) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row['project.PROJECT_NAME_DOC'],row['project.ORIGINAL_SUB_PROJECT_NAME_DOC'],row['project.SUB_PROJECT_TYPE'],row['project.LAUNCHING_TIME'],row['project.CURRENT_GRADE'],row['serviced_apartment.PROJECT_STATUS'],row['LAT'],row['LONG'],'0',City,row['location.DISTRICT_NAME_DOC'],row['developer.MAIN_DEVELOPER_NAME_DOC'],row['operator.MAIN_OPERATOR_NAME_DOC'],row['constructor.MAIN_CONSTRUCTOR_NAME_DOC'],today,'SERVICED_APARTMENT')
        elif Sector=='VLTH':
            for index, row in empty.iterrows():
                print(row)
                cursor.execute("INSERT INTO APP.D_PROJECT(PROJECT_NAME_DOC,SUB_PROJECT_CODE,SUB_PROJECT_TYPE,LAUNCHING_TIME,CURRENT_GRADE,CURRENT_STATUS,LATITUDE,LONGITUDE,W_IS_DELETED,CITY_NAME_DOC,DISTRICT_NAME_DOC,DEVELOPER_NAME_DOC,OPERATOR_NAME_DOC,CONSTRUCTOR_NAME_DOC,W_INSERT_DT,SECTOR) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row['project.PROJECT_NAME_DOC'],row['project.ORIGINAL_SUB_PROJECT_NAME_DOC'],row['project.SUB_PROJECT_TYPE'],row['project.LAUNCHING_TIME'],row['project.CURRENT_GRADE'],row['vlth.PROJECT_STATUS'],row['LAT'],row['LONG'],'0',City,row['location.DISTRICT_NAME_DOC'],row['developer.MAIN_DEVELOPER_NAME_DOC'],row['operator.MAIN_OPERATOR_NAME_DOC'],row['constructor.MAIN_CONSTRUCTOR_NAME_DOC'],today,'VLTH')
        elif Sector=='RETAIL':
            for index, row in empty.iterrows():
                cursor.execute("INSERT INTO APP.D_PROJECT(PROJECT_NAME_DOC,SUB_PROJECT_CODE,SUB_PROJECT_TYPE,LAUNCHING_TIME,CURRENT_GRADE,CURRENT_STATUS,LATITUDE,LONGITUDE,W_IS_DELETED,CITY_NAME_DOC,DISTRICT_NAME_DOC,DEVELOPER_NAME_DOC,OPERATOR_NAME_DOC,CONSTRUCTOR_NAME_DOC,W_INSERT_DT,SECTOR) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row['project.PROJECT_NAME_DOC'],row['project.ORIGINAL_SUB_PROJECT_NAME_DOC'],row['project.SUB_PROJECT_TYPE'],row['project.LAUNCHING_TIME'],row['project.CURRENT_GRADE'],row['retail.PROJECT_STATUS'],row['LAT'],row['LONG'],'0',City,row['location.DISTRICT_NAME_DOC'],row['developer.MAIN_DEVELOPER_NAME_DOC'],row['operator.MAIN_OPERATOR_NAME_DOC'],row['constructor.MAIN_CONSTRUCTOR_NAME_DOC'],today,'RETAIL')
    else:
        if Sector=='APARTMENT':                                                                                                                                                                                                                                                                                                                                                                                                  
            for index, row in empty.iterrows(): 
                print(row)
                row['Date_Key']=pd.to_datetime(row['Date_Key'],format='%Y%m%d')
                cursor.execute("INSERT INTO FRESH.Main_Sector_Project(Sector,Project_Name,Sub_Project_Name,Latitude,Longtitude,Project_Grade,Project_Type,Current_Status,Project_Phase,Project_City_Name,Project_District_Name,Developer,Developer_Nationality,Constructor,Launch_Quarter,Launch_Year,Complete_Quarter,Complete_Year,Sold_Out_Quarter,Sold_Out_Year,Date_Key,File_Date,Import_Date,Operator) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Sector,row['Project_Name'],row['Sub_Project_Name'],row['Latitude'],row['Longtitude'],row['Grade'],row['Sub_Project_Type'],row['Project_Status'],row['Project_Phase'],row['Project_City_Name'],row['Project_District_Name'],row['Developer'],row['Developer_Nationality'],row['Constructor'],row['Launch_Quarter'],row['Launch_Year'],row['Complete_Quarter'],row['Complete_Year'],row['Sold_Out_Quarter'],row['Sold_Out_Year'],row['Date_Key'],File_Date,Import_Date,row['Operator'])
        elif Sector=='RETAIL':
            for index, row in empty.iterrows():
                print(row)
                row['Date_Key']=pd.to_datetime(row['Date_Key'],format='%Y%m%d')
                cursor.execute("INSERT INTO FRESH.Main_Sector_Project(Sector,Project_Name,Sub_Project_Name,Latitude,Longtitude,Project_Type,Current_Status,Project_Phase,Project_City_Name,Project_District_Name,District_Type,Developer,Constructor,Launch_Quarter,Launch_Year,Complete_Quarter,Complete_Year,Date_Key,File_Date,Import_Date,Operator,Number_Of_Floors) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Sector,row['Project_Name'],row['Sub_Project_Name'],row['Latitude'],row['Longtitude'],row['Sub_Project_Type'],row['Project_Status'],row['Project_Phase'],row['Project_City_Name'],row['Project_District_Name'],row['District_Type'],row['Developer'],row['Constructor'],	row['Launch_Quarter'],row['Launch_Year'],row['Complete_Quarter'],row['Complete_Year'],row['Date_Key'],File_Date,Import_Date,row['Operator'],row['Number_Of_Floors'])
        elif Sector=='VLTH':
            for index, row in empty.iterrows():
                print(row)
                row['Date_Key']=pd.to_datetime(row['Date_Key'],format='%Y%m%d')
                cursor.execute("INSERT INTO FRESH.Main_Sector_Project(Sector,Project_Name,Sub_Project_Name,Latitude,Longtitude,Project_Type,Current_Status,Project_Phase,Project_City_Name,Project_District_Name,Developer,Developer_Nationality,Constructor,Constructor_Status,Launch_Quarter,Launch_Year,Complete_Quarter,Complete_Year,Sold_Out_Quarter,Sold_Out_Year,Date_Key,File_Date,Import_Date,Operator) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Sector,row['Project_Name'],row['Sub_Project_Name'],row['Latitude'],row['Longtitude'],row['Sub_Project_Type'],row['Project_Status'],row['Project_Phase'],row['Project_City_Name'],row['Project_District_Name'],row['Developer'],row['Developer_Nationality'],row['Constructor'],row['Construction_Status'],row['Launch_Quarter'],row['Launch_Year'],row['Complete_Quarter'],row['Complete_Year'],row['Sold_Out_Quarter'],row['Sold_Out_Year'],row['Date_Key'],File_Date,Import_Date,row['Operator'])
        elif Sector=='OFFICE':
            for index, row in empty.iterrows(): 
                print(row)
                row['Date_Key']=pd.to_datetime(row['Date_Key'],format='%Y%m%d')
                cursor.execute("INSERT INTO FRESH.Main_Sector_Project(Sector,Project_Name,Sub_Project_Name,Latitude,Longtitude,Project_Grade,Project_Type,Current_Status,Project_Phase,Project_City_Name,Project_District_Name,District_Type,Developer,Constructor,Launch_Quarter,Launch_Year,Complete_Quarter,Complete_Year,Date_Key,File_Date,Import_Date,Operator) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Sector,row['Project_Name'],row['Sub_Project_Name'],row['Latitude'],row['Longtitude'],row['Grade'],row['Sub_Project_Type'],row['Project_Status'],row['Project_Phase'],row['Project_City_Name'],row['Project_District_Name'],row['District_Type'],row['Developer'],row['Constructor'],row['Launch_Quarter'],row['Launch_Year'],row['Complete_Quarter'],row['Complete_Year'],row['Date_Key'],File_Date,Import_Date,row['Operator'])
        elif Sector=='HOTEL':
            for index, row in empty.iterrows(): 
                print(row)
                row['Date_Key']=pd.to_datetime(row['Date_Key'],format='%Y%m%d')
                cursor.execute("INSERT INTO FRESH.Main_Sector_Project(Sector,Project_Name,Sub_Project_Name,Latitude,Longtitude,Project_Grade,Current_Status,Project_Phase,Project_City_Name,Project_District_Name,District_Type,Developer,Developer_Nationality,Constructor,Launch_Quarter,Launch_Year,Complete_Quarter,Complete_Year,Date_Key,File_Date,Import_Date,Operator) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Sector,row['Project_Name'],row['Sub_Project_Name'],row['Latitude'],row['Longtitude'],row['Grade'],row['Project_Status'],row['Project_Phase'],row['Project_City_Name'],row['Project_District_Name'],row['District_Type'],row['Developer'],row['Developer_Nationality'],row['Constructor'],row['Launch_Quarter'],row['Launch_Year'],row['Complete_Quarter'],row['Complete_Year'],row['Date_Key'],File_Date,Import_Date,row['Operator'])
        elif Sector=='SERVICED_APARTMENT':
            for index, row in empty.iterrows(): 
                print(row)
                row['Date_Key']=pd.to_datetime(row['Date_Key'],format='%Y%m%d')
                cursor.execute("INSERT INTO FRESH.Main_Sector_Project(Sector,Project_Name,Sub_Project_Name,Latitude,Longtitude,Project_Grade,Current_Status,Project_Phase,Project_City_Name,Project_District_Name,District_Type,Developer,Developer_Nationality,Constructor,Launch_Quarter,Launch_Year,Complete_Quarter,Complete_Year,Date_Key,File_Date,Import_Date,Operator) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Sector,row['Project_Name'],row['Sub_Project_Name'],row['Latitude'],row['Longtitude'],row['Grade'],row['Project_Status'],row['Project_Phase'],row['Project_City_Name'],row['Project_District_Name'],row['District_Type'],row['Developer'],row['Developer_Nationality'],row['Constructor'],row['Launch_Quarter'],row['Launch_Year'],row['Complete_Quarter'],row['Complete_Year'],row['Date_Key'],File_Date,Import_Date,row['Operator'])
            
            

    cnxn.commit()
    cursor.close()
def Convert_District(data,cnxn):
    Raw_Type=pd.read_sql('select * from GENERAL.Project_Type_Dictionary',cnxn)
    Raw_Type['Raw_Type']=Raw_Type['Raw_Type'].str.lower()
    District=dict(zip(Raw_Type['Raw_Type'],Raw_Type['Cleaned_Type']))
    data['Sub_Project_Type']=data['Sub_Project_Type'].str.lower()
    data['Sub_Project_Type']=data['Sub_Project_Type'].replace(District)
    data['Sub_Project_Type']=data['Sub_Project_Type'].str.title()
    return data
def Get_City(city,Total_City):
     return Total_City[Total_City['Raw_City']==city].Cleaned_City.item()
def Convert_Unicode(s):
    if type(s)==str:
        s = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
        s = re.sub(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
        s = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', s)
        s = re.sub(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
        s = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
        s = re.sub(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
        s = re.sub(r'[ìíịỉĩ]', 'i', s)
        s = re.sub(r'[ÌÍỊỈĨ]', 'I', s)
        s = re.sub(r'[ùúụủũưừứựửữ]', 'u', s)
        s = re.sub(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
        s = re.sub(r'[ỳýỵỷỹ]', 'y', s)
        s = re.sub(r'[ỲÝỴỶỸ]', 'Y', s)
        s = re.sub(r'[Đ]', 'D', s)
        s = re.sub(r'[đ]', 'd', s)
        s= unicodedata.normalize('NFKD', s).encode('ascii', 'ignore')
        s=s.decode("ISO-8859-1")
    else:
        s=s
    return str(s)
def Convert_Column(data):
    data=data.apply(lambda x:Convert_Unicode(x) if x!=None else x)
    return data
def Audit(column,series,Table_Audit):
    highlight = 'color: red;'
    default = ''
    return [highlight if e!=None and Table_Audit[column][i]!=round(e,4) else default for i,e in enumerate(series)]
def Compare(series,Table_Audit):
    if series.name=='Record':
        return Audit('Record',series,Table_Audit)
    elif series.name=='Total_Supply':
        return Audit('Total_Supply',series,Table_Audit)
    elif series.name=='Accumulated_Launched':
        return Audit('Accumulated_Launched',series,Table_Audit)
    elif series.name=='Available_Units':
        return Audit('Available_Units',series,Table_Audit)
    elif series.name=='Quarterly_Sales':
        return Audit('Quarterly_Sales',series,Table_Audit)
    elif series.name=='LA_Primary_Price':
        return Audit('LA_Primary_Price',series,Table_Audit)
    elif series.name=='Leased':
        return Audit('Leased',series,Table_Audit)
    elif series.name=='Accumulated_Sold':
        return Audit('Accumulated_Sold',series,Table_Audit)
    elif series.name=='Unit_Price':
        return Audit('Unit_Price',series,Table_Audit)
#So sánh 2 table

