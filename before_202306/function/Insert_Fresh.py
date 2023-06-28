from urllib.parse import quote_plus
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, event
import re
import pyodbc
import unicodedata
def convert_float(x):
    try:
        return float(x)
    except:
        return None
def convert_int(x):
    try:
        return int(x)
    except:
        return None
def convert_round(x):
    try:
        return round(x)
    except:
        return None
def Insert_to_fresh(Sector,cnxn,data,project_name,File_Date):
    # azure sql connect tion string
    conn ='Driver={ODBC Driver 17 for SQL Server};Server=tcp:hkazdevsqld3vnreserch.database.windows.net,1433;Database=D3VNResearch_Staging;Uid=D3VNResearch@savills.com.vn;Pwd=Wy6bF!4ncy6U+vxN;Encrypt=yes;Authentication=ActiveDirectoryPassword;Connection Timeout=30;'
    quoted = quote_plus(conn)
    engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("Insert to Fresh")
        if executemany:
            cursor.fast_executemany = True
    today=pd.to_datetime('today')
    for i in list(data.columns):
            data[i] = data[i].replace({np.nan: None})
    data['Project_Name']=Convert_Column(data['Project_Name'])
    data['Sub_Project_Name']=Convert_Column(data['Sub_Project_Name'])
    data['Developer']=Convert_Column(data['Developer'])
    data['Operator']=Convert_Column(data['Operator'])
    data['Project_District_Name']=Convert_Column(data['Project_District_Name'])
    data['Import_date']=today
    data['File_Name']=project_name
    data['File_Date']=File_Date
    if Sector=='RETAIL':
        table_name='RETAIL'
        data['GFA']=data['GFA'].apply(convert_float)
        data['NLA']=data['NLA'].apply(convert_float)
        data['Vaccant_Area']=data['Vaccant_Area'].apply(convert_float)
        data['Leased_Area']=data['Leased_Area'].apply(convert_float)
        data['Avg_Gross_Rent']=data['Avg_Gross_Rent'].apply(convert_float)
        data['Service_Charge']=data['Service_Charge'].apply(convert_float)
        data['Number_Of_Floors']=data['Number_Of_Floors'].apply(convert_float)
        data['Launch_Quarter']=data['Launch_Quarter'].apply(convert_int)
        data['Complete_Quarter']=data['Complete_Quarter'].apply(convert_int)
        #for i, row in data.iterrows():
            #print(row)
            #data.iloc[i:i+1].to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='APARTMENT':
        table_name='APT'
        #data=data.iloc[:,0:40]
        data['Min_Size_Area']=data['Min_Size_Area'].apply(convert_float)
        data['Max_Size_Area']=data['Max_Size_Area'].apply(convert_float)
        data['Sold_Units_End_Of_Q']=data['Sold_Units_End_Of_Q'].apply(convert_float)
        data['Available_Units']=data['Available_Units'].apply(convert_float)
        data['UNIT_Secondary_Price']=data['UNIT_Secondary_Price'].apply(convert_float)
        data['Quarterly_Sales']=data['Quarterly_Sales'].apply(convert_float)
        data['Avg_Secondary_Price']=data['Avg_Secondary_Price'].apply(convert_float)
        data['Avg_Primary_Price']=data['Avg_Primary_Price'].apply(convert_float)
        data['Min_Size_Area']=data['Min_Size_Area'].apply(convert_float)
        data['Land_Area']=data['Land_Area'].apply(convert_float)
        data['Total_Launched_Units']=data['Total_Launched_Units'].apply(convert_float)
        data['Total_Sold_Units']=data['Total_Sold_Units'].apply(convert_float)
        data['Total_Supply']=data['Total_Supply'].apply(convert_float)
        data['NFA_Secondary_Price']=data['NFA_Secondary_Price'].apply(convert_float)
        print(data.columns)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='HOTEL':
        table_name='HOTEL'
        data['Total_Supply']=data['Total_Supply'].apply(convert_float)
        data['Leased_Rooms']=data['Leased_Rooms'].apply(convert_round)
        data['Avg_Room_Price']=data['Avg_Room_Price'].apply(convert_float)
        data['Launch_Quarter']=data['Launch_Quarter'].apply(convert_float)
        data['Complete_Quarter']=data['Complete_Quarter'].apply(convert_float)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='OFFICE':
        table_name='OFFICE'
        data['Avg_Gross_Rent']=data['Avg_Gross_Rent'].apply(convert_float)
        data['Leased_Area_End_Of_Q']=data['Leased_Area_End_Of_Q'].apply(convert_float)
        data['Average_Price']=data['Average_Price'].apply(convert_float)
        data['Actual_Net_Rent']=data['Actual_Net_Rent'].apply(convert_float)
        data['NLA']=data['NLA'].apply(convert_float)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='SERVICED_APARTMENT':
        table_name='Serviced_Apartment'
        data['Avg_Unit_Size']=data['Avg_Unit_Size'].apply(convert_float)
        data['Rev_PAU _m2']=data['Rev_PAU _m2'].apply(convert_float)
        data['Rev_PAU']=data['Rev_PAU'].apply(convert_float)
        data['Rent_Per_Unit']=data['Rent_Per_Unit'].apply(convert_float)
        data['Total_Supply']=data['Total_Supply'].apply(convert_float)
        data['Net_Lettable']=data['Net_Lettable'].apply(convert_float)
        data['Avg_Rent']=data['Avg_Rent'].apply(convert_float)
        data['Leased_Units']=data['Leased_Units'].apply(convert_float)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='VLTH':
        table_name='VLTH'
        data['Available_Units']=data['Available_Units'].apply(convert_float)
        data['Quarterly_Sales']=data['Quarterly_Sales'].apply(convert_float)
        data['Total_Sold_Units']=data['Total_Sold_Units'].apply(convert_float)
        data['Total_Supply']=data['Total_Supply'].apply(convert_float)
        data['Remaining_Supply']=data['Remaining_Supply'].apply(convert_float)
        data['LA_Primary_Price']=data['LA_Primary_Price'].apply(convert_float)
        data['LA_Secondary_Price']=data['LA_Secondary_Price'].apply(convert_float)
        data['UNIT_Primary_Price']=data['UNIT_Primary_Price'].apply(convert_float)
        data['UNIT_Secondary_Price']=data['UNIT_Secondary_Price'].apply(convert_float)

        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
def Cleaned_Data(data,cnxn,table_name):
    #Convert sang đúng district
    Raw_dictrict=pd.read_sql('select * from GENERAL.District_Dictionary',cnxn)
    District=dict(zip(Raw_dictrict['Raw_District'],Raw_dictrict['Cleaned_District']))
    Project_District={key.strip():value for key,value in District.items()}
    data['Project_District_Name']=data['Project_District_Name'].str.strip()
    data['Project_District_Name']=data['Project_District_Name'].replace(Project_District)
    #Convert Subtype ->> example: Centre->Center
    if table_name in ['VLTH','RETAIL']:
        Sub_type=pd.read_sql('select * from GENERAL.Project_Type_Dictionary',cnxn)
        District=dict(zip(Sub_type['Raw_Type'],Sub_type['Cleaned_Type']))
        data['Sub_Project_Type']=data['Sub_Project_Type'].replace(District)
    #Convert sang City
    Raw_City =pd.read_sql('SELECT * FROM GENERAL.City_Dictionary',cnxn)
    City=dict(zip(Raw_City['Raw_City'],Raw_City['Cleaned_City']))
    data['Project_City_Name']=data['Project_City_Name'].replace(City)
    return data
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
def convert_float1(x):
    if x!=None:
        x=str(x).replace(',','')
    try:
        return float(x)
    except:
        return None
def convert_int1(x):
    if x!=None:
        x=str(x).replace(',','')
    try:
        return int(x)
    except:
        return None
def Insert_to_fresh_Optimal(Sector,cnxn,data,project_name,File_Date,today):
    # azure sql connect tion string
    conn ='Driver={ODBC Driver 17 for SQL Server};Server=tcp:hkazdevsqld3vnreserch.database.windows.net,1433;Database=D3VNResearch_Staging;Uid=D3VNResearch@savills.com.vn;Pwd=Wy6bF!4ncy6U+vxN;Encrypt=yes;Authentication=ActiveDirectoryPassword;Connection Timeout=30;'
    quoted = quote_plus(conn)
    engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("Insert to Fresh")
        if executemany:
            cursor.fast_executemany = True
    for i in list(data.columns):
            data[i] = data[i].replace({np.nan: None})
    data['Project_Name']=Convert_Column(data['Project_Name'])
    data['Sub_Project_Name']=Convert_Column(data['Sub_Project_Name'])
    data['Developer']=Convert_Column(data['Developer'])
    data['Operator']=Convert_Column(data['Operator'])
    data['Project_District_Name']=Convert_Column(data['Project_District_Name'])
    data['Import_date']=today
    data['File_Name']=project_name
    data['File_Date']=File_Date
    if Sector=='RETAIL':
        table_name='RETAIL'
        columns_float=['Longtitude','Latitude','Service_Charge','Avg_Gross_Rent','Leased_Area','Vaccant_Area','NLA','GFA','Number_Of_Floors','New_Supply']
        for column_f in columns_float:
            #data[column_f]=data[column_f].replace({',':''})
            data[column_f]=data[column_f].apply(convert_float1)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='APARTMENT':
        table_name='APT'
        columns_float=['CFA_Primary_Price','Longtitude','Latitude','Land_Area','Min_Size_Area','Max_Size_Area','Secondary_Supply','Total_Supply','Total_Launched_Units','Available_Units','Total_Sold_Units','Sold_Units_End_Of_Q','Quarterly_Sales','Area_NFA_Max','Area_NFA_Min','Area_GFA_Max','Area_GFA_Min','NFA_Primary_Price','Avg_Primary_Price','NFA_Secondary_Price','Avg_Secondary_Price','Quarterly_Sales_New_Supply']
        columns_int=['Project_Phase','Launch_Quarter' ,'Sold_Out_Quarter','Complete_Quarter']
        for column_f in columns_float:
            data[column_f]=data[column_f].apply(convert_float1)
        for column_int in columns_int:
            data[column_int]=data[column_int].apply(convert_int)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='HOTEL':
        table_name='HOTEL'
        columns_float=['Longtitude','Latitude','Avg_Room_Price','Occupancy','Total_Supply','Land_Area','New_Supply']
        for column_f in columns_float:
            data[column_f]=data[column_f].apply(convert_float1)
        #data['Leased_Rooms']=data['Leased_Rooms'].apply(convert_round)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='OFFICE':
        table_name='OFFICE'
        #Vaccant_Area
        columns_float=['Longtitude','Latitude','Avg_Gross_Rent','Leased_Area_End_Of_Q','NLA','Vacant_Area','Gross_Office_Area','Efficiency','New_Supply']
        for column_f in columns_float:
            data[column_f]=data[column_f].apply(convert_float1)
        #data['Actual_Net_Rent']=data['Actual_Net_Rent'].apply(convert_float)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='SERVICED_APARTMENT':
        table_name='Serviced_Apartment'
        columns_float=['Longtitude','Latitude','Avg_Unit_Size','Rent_Per_Unit','Avg_Rent','Rev_PAU _m2','Net_Lettable','Leased_Units','Launch_Units','Total_Supply','New_Supply']
        for column_f in columns_float:
            data[column_f]=data[column_f].apply(convert_float1)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif Sector=='VLTH':
        table_name='VLTH'
        columns_float=['Longtitude','Latitude','UNIT_Secondary_Price','LA_Secondary_Price','UNIT_Primary_Price','LA_Primary_Price','Remaining_Supply','Quarterly_Sales','Sold_Units_End_Of_Q','Total_Sold_Units','Available_Units','Total_Launched_Units','Total_Supply','Land_Area','Quarterly_Sales_New_Supply']
        for column_f in columns_float:
            data[column_f]=data[column_f].apply(convert_float1)
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')

    