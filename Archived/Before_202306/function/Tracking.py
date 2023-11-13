import pandas as pd
import numpy as np
import os 
import glob
import csv
from datetime import date
import re

def convert_int(x):
    try:
        return float(x)
    except:
        return None
convert=lambda x: str(x).replace(',','').replace(' -   ','') if  x!=np.nan or x not in ('  ') else x
def summ_flat_file(obj_sp, dataframe, file,crsr,Raw_City):
    today = pd.to_datetime("today")
    print(file)
    project_name=file.split('/')[-1]
    sector=project_name.upper().split('_')[0]
    print(sector)
    raw_city=project_name.split('_')[1]
    City=Raw_City[raw_city]
    if sector=='SA':
        sector='SERVICED_APARTMENT'
    elif sector=='APT':
        sector='APARTMENT'
    text=obj_sp.get_file(file)
    text=text.dropna(subset=['Form'])
    text=text.replace(' -   ',0).replace('    ',np.nan)
    text.columns=text.columns.str.strip()
    history=dataframe[dataframe.Name==project_name]
    Record=text.shape[0]
    Modified=history.iloc[0,3]
    Modified_By=int(history.iloc[0,4])
    if sector=='OFFICE':
        DATE_KEY=pd.to_datetime(text['Date_Key'].iloc[0],format='%Y%m%d')
        text['NLA']=text['NLA'].apply(convert)
        text['Leased_Area_End_Of_Q']=text['Leased_Area_End_Of_Q'].apply(convert)
        text['Avg_Gross_Rent']=text['Avg_Gross_Rent'].apply(convert)
        # text['office.OCCUPANCY']= text['office.OCCUPANCY'].apply(convert)  ## hỏi lại 
        for i in text.columns:
            text[i]=text[i].apply(convert_int)
        NLA=text.loc[:,'NLA'].sum()
        LEASED=text.loc[:,'Leased_Area_End_Of_Q'].sum()
        Avg_Gross_Rent=text.loc[:,'Avg_Gross_Rent'].sum()
        # OCCUPANCY=text.loc[:,'office.OCCUPANCY'].sum()
        crsr.execute("INSERT INTO FRESH.Main_Sector_Tracking_Flat_File(File_Input, Data_Date, Modified_Date, Import_Date, Modified_By, Sector, Record, NLA, Leased, City,Avg_Gross_Rent)  values(?,?,?,?,?,?,?,?,?,?,?)", project_name, DATE_KEY,Modified,today ,Modified_By,sector, Record,NLA,LEASED,City,Avg_Gross_Rent)
    elif sector=='RETAIL':
        DATE_KEY=pd.to_datetime(text['Date_Key'].iloc[0],format='%Y%m%d')
        text['NLA']=text['NLA'].apply(convert)
        text['Leased_Area']=text['Leased_Area'].apply(convert)
        # text['Occupancy']=text['Occupancy'].apply(convert)
        for i in text.columns:
            text[i]=text[i].apply(convert_int)
        NLA=text.loc[:,'NLA'].sum()
        LEASED=text.loc[:,'Leased_Area'].sum()
    # OCCUPANCY=text.loc[:,'Occupancy'].sum()
        crsr.execute("INSERT INTO FRESH.Main_Sector_Tracking_Flat_File(File_Input, Data_Date, Modified_Date, Import_Date, Modified_By, Sector, Record, NLA, Leased, City) values(?,?,?,?,?,?,?,?,?,?)",project_name, DATE_KEY,Modified,today ,Modified_By,sector, Record,NLA,LEASED,City)
    elif sector=='HOTEL':
        DATE_KEY=pd.to_datetime(text['Date_Key'].iloc[0],format='%Y%m%d')
        text['Total_Supply']=text['Total_Supply'].apply(convert)
        text['Leased_Rooms']=text['Leased_Rooms'].apply(convert)
        text['Avg_Room_Price']=text['Avg_Room_Price'].apply(convert)
        text['Occupancy']=text['Occupancy'].apply(convert)
        # text['hotel.OCCUPANCY']=text['hotel.OCCUPANCY'].apply(convert)
        for i in text.columns:
            text[i]=text[i].apply(convert_int)
        ToTalSupply=int(text.loc[:,'Total_Supply'].sum())
        LEASED=text.loc[:,'Leased_Rooms'].sum()
        ADR=text.loc[:,'Avg_Room_Price'].sum()
        Occu=text.loc[:,'Occupancy'].sum()
        # OCCUPANCY=text.loc[:,'hotel.OCCUPANCY'].sum()
        crsr.execute("INSERT INTO FRESH.Main_Sector_Tracking_Flat_File(File_Input, Data_Date, Modified_Date, Import_Date, Modified_By, Sector, Record, Total_Supply, Leased, Avg_Gross_Rent, City,Occupancy)  values(?,?,?,?,?,?,?,?,?,?,?,?)",project_name, DATE_KEY,Modified,today,Modified_By,sector,Record ,ToTalSupply ,LEASED, ADR,City,Occu)
    elif sector=='APARTMENT':
        DATE_KEY=pd.to_datetime(text['Date_Key'].iloc[0],format='%Y%m%d')
        text['Total_Supply']=text['Total_Supply'].apply(convert)
        text['Total_Launched_Units']=text['Total_Launched_Units'].apply(convert)
        text['Land_Area']=text['Land_Area'].apply(convert)
        # text['apartment.ACCUMULATED_SOLD_UNITS_ADJUSTED']=text['apartment.ACCUMULATED_SOLD_UNITS_ADJUSTED'].apply(convert).astype(float)
        text['Total_Sold_Units']=text['Total_Sold_Units'].apply(convert)
        text['Available_Units']=text['Available_Units'].apply(convert)
        text['Quarterly_Sales']=text['Quarterly_Sales'].apply(convert)
        text['Avg_Primary_Price']=text['Avg_Primary_Price'].apply(convert)
        #text['NFA_Primary_Price']=text['NFA_Primary_Price'].apply(convert)
        for i in text.columns:
            text[i]=text[i].apply(convert_int)
        ToTalSupply=int(text.loc[:,'Total_Supply'].sum())
        Launched_Unit=int(text.loc[:,'Total_Launched_Units'].sum())
        Sold_Unit=text.loc[:,'Total_Sold_Units'].sum()
        # Sold_Unit_Adjusted=text.loc[:,'apartment.ACCUMULATED_SOLD_UNITS_ADJUSTED'].sum()
        Available_Unit=int(text.loc[:,'Available_Units'].sum())
        Quaterly_Sales=text.loc[:,'Quarterly_Sales'].sum()
        Primary_Price=text.loc[:,'NFA_Primary_Price'].sum()
        print(Primary_Price)
        crsr.execute("INSERT INTO FRESH.Main_Sector_Tracking_Flat_File(File_Input, Data_Date, Modified_Date, Import_Date, Modified_By, Sector, Record, Total_Supply, Accumulated_Launched, Accumulated_Sold, Available_Units, Quarterly_Sales, LA_Primary_Price, City) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",project_name, DATE_KEY,Modified,today ,Modified_By,sector, int(Record), ToTalSupply, Launched_Unit, Sold_Unit, Available_Unit, Quaterly_Sales, Primary_Price, City)
    elif sector=='VLTH':
        DATE_KEY=pd.to_datetime(text['Date_Key'].iloc[0],format='%Y%m%d')
        text['Total_Supply']=text['Total_Supply'].apply(convert)
        text['Total_Launched_Units']=text['Total_Launched_Units'].apply(convert)
        text['Total_Sold_Units']=text['Total_Sold_Units'].apply(convert)
        text['Available_Units']=text['Available_Units'].apply(convert)
        text['Quarterly_Sales']=text['Quarterly_Sales'].apply(convert)
        text['LA_Primary_Price']=text['LA_Primary_Price'].apply(convert)
        text['UNIT_Primary_Price']=text['UNIT_Primary_Price'].apply(convert)
        for i in text.columns:
            text[i]=text[i].apply(convert_int)
        ToTalSupply=int(text.loc[:,'Total_Supply'].sum())
        Launched_Unit=int(text.loc[:,'Total_Launched_Units'].sum())
        Sold_Unit_Adjusted=int(text.loc[:,'Total_Sold_Units'].sum())
        Available_Unit=int(text.loc[:,'Available_Units'].sum())
        Quaterly_Sales=int(text.loc[:,'Quarterly_Sales'].sum())
        Primary_Price=text.loc[:,'LA_Primary_Price'].sum()
        UNIT=text.loc[:,'UNIT_Primary_Price'].sum()
        crsr.execute("INSERT INTO FRESH.Main_Sector_Tracking_Flat_File(File_Input,Data_Date,Modified_Date,Import_Date,Modified_By,Sector,Record,Total_Supply,Accumulated_Launched,Accumulated_Sold_Adj,Available_Units,Quarterly_Sales,LA_Primary_Price,Unit_Price,City)  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",project_name,DATE_KEY, Modified,today,Modified_By,sector,Record,ToTalSupply,Launched_Unit, Sold_Unit_Adjusted ,Available_Unit, Quaterly_Sales,Primary_Price ,UNIT,City)
    elif sector=='SERVICED_APARTMENT':
        DATE_KEY=pd.to_datetime(text['Date_Key'].iloc[0],format='%Y%m%d')
        text['Avg_Rent']=text['Avg_Rent'].apply(convert)
        text['Total_Supply']=text['Total_Supply'].apply(convert)
        text['Leased_Units']=text['Leased_Units'].apply(convert)
        # text['Occupancy']=text['Occupancy'].apply(convert)
        for i in text.columns:
            text[i]=text[i].apply(convert_int)
        ToTalSupply=int(text.loc[:,'Total_Supply'].sum())
        LEASED=text.loc[:,'Leased_Units'].sum()
        AVERAGE_GROSS_RENT=text.loc[:,'Avg_Rent'].sum()
        # OCCUPANCY=text.loc[:,'Occupancy'].sum()
        crsr.execute("INSERT INTO FRESH.Main_Sector_Tracking_Flat_File(File_Input,Data_Date,Modified_Date,Import_Date,Modified_By,Sector,Record,Total_Supply,Leased,Avg_Gross_Rent,City)  values(?,?,?,?,?,?,?,?,?,?,?)",project_name,DATE_KEY,Modified,today,Modified_By,sector,Record,ToTalSupply,LEASED, AVERAGE_GROSS_RENT,City)
def tracking_fact(cnxn, ls_sectors=[False], ls_city=[False] ,ls_year=[False] ,ls_quarter=[False], today_Q=0):

    ## Read summary of fact table
    sql_query_fact = "SELECT COUNT(t1.PROJECT_KEY) as Record, SUM(t1.Total_Supply) as Total_Supply, NULL as NLA, NULL as GFA, NULL as Leased,NULL as Avg_Gross_Rent, sum(t1.Total_Launched_Units) as Accumulated_Launched,sum(t1.Available_Units) as Available_Units,NULL as Accumulated_Sold,sum(t1.Quarterly_Sales) as Quarterly_Sales,sum(t1.LA_Primary_Price) as LA_Primary_Price,SUM(t1.UNIT_Primary_Price) as Unit_Price,Null as Occupancy, Sector = 'VLTH', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.VLTH t1\
    JOIN DWH.D_DATE f ON t1.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t1.Project_Key = g.Project_Key\
    WHERE t1.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL\
    SELECT COUNT(t2.PROJECT_KEY) as Record, SUM(t2.Total_Supply) as Total_Supply, NULL, NULL, NULL, NULL, sum(t2.Total_Launched_Units) as Accumulated_Launched, sum(t2.Available_Units) as Available_Units, sum(t2.Total_Sold_Units) as Accumulated_Sold,sum(t2.Quarterly_Sales) as Quarterly_Sales,sum(t2.NFA_Primary_Price) as LA_Primary_Price,null,Null as Occupancy, Sector = 'APARTMENT', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.APT t2\
    JOIN DWH.D_DATE f ON t2.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t2.Project_Key = g.Project_Key\
    WHERE t2.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL \
    SELECT COUNT(t3.PROJECT_KEY) as Record, SUM(t3.Total_Supply) as Total_Supply,NULL,NULL, sum(t3.Leased_Units) as Leased, sum(t3.Avg_Rent) as Avg_Gross_Rent , NULL,NULL,NULL,NULL,NULL,null,Null as Occupancy, Sector = 'SERVICED_APARTMENT', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.Serviced_Apartment t3\
    JOIN DWH.D_DATE f ON t3.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t3.Project_Key = g.Project_Key\
    WHERE t3.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL\
    SELECT COUNT(t4.PROJECT_KEY) as Record, SUM(t4.Total_Supply) as Total_Supply,NULL, NULL, sum(t4.Leased_Rooms) as Leased,sum(t4.Avg_Room_Price) as Avg_Gross_Rent , null,null,null,NULL,null,null,sum(Occupancy) as Occupancy, Sector='HOTEL', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.Hotel t4\
    JOIN DWH.D_DATE f ON t4.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t4.Project_Key = g.Project_Key\
    WHERE t4.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL\
    SELECT COUNT(t5.PROJECT_KEY) as Record, NULL, SUM(t5.NLA) as NLA, NULL, SUM(t5.Leased_Area) AS Leased ,NULL, NULL,NULL,NULL,NULL,NULL,NULL,Null as Occupancy, Sector= 'RETAIL', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.Retail t5\
    JOIN DWH.D_DATE f ON t5.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t5.Project_Key = g.Project_Key\
    WHERE t5.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL\
    SELECT COUNT(t6.PROJECT_KEY) as Record, null,sum(t6.NLA) as NLA, NULL, sum(t6.Leased_Area_End_Of_Q) as Leased, sum(t6.Avg_Gross_Rent) as Avg_Gross_Rent, null,null,null,null,null,null,Null as Occupancy, Sector='OFFICE', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.Office t6\
    JOIN DWH.D_DATE f ON t6.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t6.Project_Key = g.Project_Key\
    WHERE t6.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER"

    summ_fact = pd.read_sql(sql_query_fact%(today_Q,today_Q,today_Q,today_Q,today_Q,today_Q),cnxn)
    info_inserted_meta = pd.DataFrame({'Sector':ls_sectors ,'City':ls_city,'YEAR':ls_year, 'QUARTER':ls_quarter})
    info_inserted_meta = info_inserted_meta.drop(columns=info_inserted_meta.columns[(info_inserted_meta == False).any()])
    if info_inserted_meta.empty == True:
        pass
    else:
        summ_fact = pd.merge(summ_fact,info_inserted_meta, how='inner', on=info_inserted_meta.columns.to_list(), indicator=False)
    summ_fact_copy = summ_fact.round(4).copy() # This copy aim to be an intermediary for data processing
    ## Read summary of general tracking flat file
    sql_query = "SELECT Record, Total_Supply, NLA, GFA, Leased, Avg_Gross_Rent, Accumulated_Launched, Available_Units,  Accumulated_Sold, Quarterly_Sales, LA_Primary_Price, Unit_Price,Occupancy, Sector, City, YEAR, QUARTER FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY t.Sector, City, YEAR, QUARTER ORDER BY Import_Date DESC) AS RowNumber\
        FROM FRESH.Main_Sector_Tracking_Flat_File t\
        LEFT JOIN DWH.D_DATE g\
        ON t.Data_Date = g.DATE) t2\
        WHERE RowNumber = 1 AND City IS NOT NULL AND YEAR IS NOT NULL AND QUARTER IS NOT NULL"
    df_GeneralTracking = pd.read_sql(sql_query,cnxn)
    df_GeneralTracking = df_GeneralTracking.round(4)
    ## Process to map or match 2 table
    Audit=pd.read_sql(sql_query,cnxn)
    Audit=Audit.round(4).copy() 
    for i in list(df_GeneralTracking.columns):
        df_GeneralTracking[i] = df_GeneralTracking[i].replace({np.nan: 0})
        summ_fact_copy[i] = summ_fact_copy[i].replace({np.nan: 0})
        summ_fact[i] = summ_fact[i].replace({np.nan: None})
    df_GeneralTracking=pd.merge(df_GeneralTracking,info_inserted_meta, how='inner', on=info_inserted_meta.columns.to_list(), indicator=False)
    cross_check = pd.merge(df_GeneralTracking,summ_fact_copy, how='inner', indicator=False)
    cross_check['W_MATCH'] = '1'
    summ_fact = summ_fact.reset_index(drop=True)
    summ_fact['W_MATCH'] = summ_fact.merge(cross_check, how='left', on=['Sector','City', 'YEAR', 'QUARTER'])['W_MATCH']
    summ_fact['W_MATCH'] = summ_fact['W_MATCH'].replace({np.nan: 0})
    Audit = pd.merge(Audit,info_inserted_meta, how='inner', on=info_inserted_meta.columns.to_list(), indicator=False)
    if ls_sectors == None:
        pass
    else:
        Table_Audit = Audit[Audit['Sector'].isin(list(set(ls_sectors)))]

    if ls_city == None:
        pass
    else:
        Table_Audit = Table_Audit[Table_Audit['City'].isin(list(set(ls_city)))]

    if ls_year == None:
        pass
    else:
        Table_Audit = Table_Audit[Table_Audit['YEAR'].isin(list(set(ls_year)))]

    if ls_quarter == None:
        pass
    else:
        Table_Audit = Table_Audit[Table_Audit['QUARTER'].isin(list(set(ls_quarter)))]

    return summ_fact, cross_check,Table_Audit

#Làm tròn(4 số sau thập phân) và highlight những value bị sai
def Audit(column,series,Table_Audit):
    highlight = 'color: red;'
    default = ''
    return [highlight if e!=None and Table_Audit[column][i]!=round(e,4) else default for i,e in enumerate(series)]
def Compare(series,Table_Audit):
    if series.name=='Record':
        return Audit('Record',series,Table_Audit)
    elif series.name=='TOTAL_SUPPLY':
        return Audit('TOTAL_SUPPLY',series,Table_Audit)
    elif series.name=='NLA':
        return Audit('NLA',series,Table_Audit)
    elif series.name=='Leased':
        return Audit('Leased',series,Table_Audit)
    elif series.name=='Occupancy':
        return Audit('Occupancy',series,Table_Audit)
    elif series.name=='Avg_Gross_Rent':
        return Audit('Avg_Gross_Rent',series,Table_Audit)
    elif series.name=='Accumulated_Launched':
        return Audit('Accumulated_Launched',series,Table_Audit)
    elif series.name=='Available_Units':
        return Audit('Available_Units',series,Table_Audit)
    elif series.name=='Accumulated_Sold':
        return Audit('Accumulated_Sold',series,Table_Audit)
    elif series.name=='Accumulated_Sold_Adj':
        return Audit('Accumulated_Sold_Adj',series,Table_Audit)
    elif series.name=='QUARTERLY_SALES':
        return Audit('QUARTERLY_SALES',series,Table_Audit)
    elif series.name=='LA_Primary_Price':
        return Audit('LA_Primary_Price',series,Table_Audit)
    elif series.name=='Unit_Price':
        return Audit('Unit_Price',series,Table_Audit)
def Tracking_Fact_Main_Sector(cnxn, ls_sectors=[False], ls_city=[False] ,ls_year=[False] ,ls_quarter=[False], today_Q=0):

    ## Read summary of fact table
    sql_query_fact = "SELECT COUNT(t1.PROJECT_KEY) as Record, SUM(t1.Total_Supply) as Total_Supply, NULL as NLA, NULL as GFA, NULL as Leased,NULL as Avg_Gross_Rent, sum(t1.Total_Launched_Units) as Accumulated_Launched,sum(t1.Available_Units) as Available_Units,NULL as Accumulated_Sold,sum(t1.Quarterly_Sales) as Quarterly_Sales,sum(t1.LA_Primary_Price) as LA_Primary_Price,SUM(t1.UNIT_Primary_Price) as Unit_Price,Null as Occupancy, Sector = 'VLTH', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.VLTH t1\
    JOIN DWH.D_DATE f ON t1.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t1.Project_Key = g.Project_Key\
    WHERE t1.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL\
    SELECT COUNT(t2.PROJECT_KEY) as Record, SUM(t2.Total_Supply) as Total_Supply, NULL, NULL, NULL, NULL, sum(t2.Total_Launched_Units) as Accumulated_Launched, sum(t2.Available_Units) as Available_Units, sum(t2.Total_Sold_Units) as Accumulated_Sold,sum(t2.Quarterly_Sales) as Quarterly_Sales,sum(t2.NFA_Primary_Price) as LA_Primary_Price,null,Null as Occupancy, Sector = 'APARTMENT', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.APT t2\
    JOIN DWH.D_DATE f ON t2.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t2.Project_Key = g.Project_Key\
    WHERE t2.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL \
    SELECT COUNT(t3.PROJECT_KEY) as Record, SUM(t3.Total_Supply) as Total_Supply,NULL,NULL, sum(t3.Leased_Units) as Leased, sum(t3.Avg_Rent) as Avg_Gross_Rent , NULL,NULL,NULL,NULL,NULL,null,Null as Occupancy, Sector = 'SERVICED_APARTMENT', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.Serviced_Apartment t3\
    JOIN DWH.D_DATE f ON t3.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t3.Project_Key = g.Project_Key\
    WHERE t3.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL\
    SELECT COUNT(t4.PROJECT_KEY) as Record, SUM(t4.Total_Supply) as Total_Supply,NULL, NULL, sum(t4.Leased_Rooms) as Leased,sum(t4.Avg_Room_Price) as Avg_Gross_Rent , null,null,null,NULL,null,null,sum(Occupancy) as Occupancy, Sector='HOTEL', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.Hotel t4\
    JOIN DWH.D_DATE f ON t4.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t4.Project_Key = g.Project_Key\
    WHERE t4.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL\
    SELECT COUNT(t5.PROJECT_KEY) as Record, NULL, SUM(t5.NLA) as NLA, NULL, SUM(t5.Leased_Area) AS Leased ,NULL, NULL,NULL,NULL,NULL,NULL,NULL,Null as Occupancy, Sector= 'RETAIL', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.Retail t5\
    JOIN DWH.D_DATE f ON t5.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t5.Project_Key = g.Project_Key\
    WHERE t5.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER\
    UNION ALL\
    SELECT COUNT(t6.PROJECT_KEY) as Record, null,sum(t6.NLA) as NLA, NULL, sum(t6.Leased_Area_End_Of_Q) as Leased, sum(t6.Avg_Gross_Rent) as Avg_Gross_Rent, null,null,null,null,null,null,Null as Occupancy, Sector='OFFICE', g.Project_City_Name as City, f.YEAR, f.QUARTER\
    FROM FRESH.Office t6\
    JOIN DWH.D_DATE f ON t6.Date_Key = f.Date_Key\
    JOIN FRESH.Main_Sector_Project g ON t6.Project_Key = g.Project_Key\
    WHERE t6.Import_date = '%s'\
    GROUP BY g.Project_City_Name, f.YEAR, f.QUARTER"

    summ_fact = pd.read_sql(sql_query_fact%(today_Q,today_Q,today_Q,today_Q,today_Q,today_Q),cnxn)
    info_inserted_meta = pd.DataFrame({'Sector':ls_sectors ,'City':ls_city,'YEAR':ls_year, 'QUARTER':ls_quarter})
    info_inserted_meta = info_inserted_meta.drop(columns=info_inserted_meta.columns[(info_inserted_meta == False).any()])
    if info_inserted_meta.empty == True:
        pass
    else:
        summ_fact = pd.merge(summ_fact,info_inserted_meta, how='inner', on=info_inserted_meta.columns.to_list(), indicator=False)
    summ_fact_copy = summ_fact.round(4).copy() # This copy aim to be an intermediary for data processing
    ## Read summary of general tracking flat file
    sql_query = "SELECT Record, Total_Supply, NLA, GFA, Leased, Avg_Gross_Rent, Accumulated_Launched, Available_Units,  Accumulated_Sold, Quarterly_Sales, LA_Primary_Price, Unit_Price,Occupancy, Sector, City, YEAR, QUARTER FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY t.Sector, City, YEAR, QUARTER ORDER BY Import_Date DESC) AS RowNumber\
        FROM FRESH.Main_Sector_Tracking_Flat_File t\
        LEFT JOIN DWH.D_DATE g\
        ON t.Data_Date = g.DATE) t2\
        WHERE RowNumber = 1 AND City IS NOT NULL AND YEAR IS NOT NULL AND QUARTER IS NOT NULL"
    df_GeneralTracking = pd.read_sql(sql_query,cnxn)
    df_GeneralTracking = df_GeneralTracking.round(4)
    ## Process to map or match 2 table
    Audit=pd.read_sql(sql_query,cnxn)
    Audit=Audit.round(4).copy() 
    for i in list(df_GeneralTracking.columns):
        df_GeneralTracking[i] = df_GeneralTracking[i].replace({np.nan: 0})
        summ_fact_copy[i] = summ_fact_copy[i].replace({np.nan: 0})
        summ_fact[i] = summ_fact[i].replace({np.nan: None})
    df_GeneralTracking=pd.merge(df_GeneralTracking,info_inserted_meta, how='inner', on=info_inserted_meta.columns.to_list(), indicator=False)
    cross_check = pd.merge(df_GeneralTracking,summ_fact_copy, how='inner', indicator=False)
    cross_check['W_MATCH'] = '1'
    summ_fact = summ_fact.reset_index(drop=True)
    summ_fact['W_MATCH'] = summ_fact.merge(cross_check, how='left', on=['Sector','City', 'YEAR', 'QUARTER'])['W_MATCH']
    summ_fact['W_MATCH'] = summ_fact['W_MATCH'].replace({np.nan: 0})
    Audit = pd.merge(Audit,info_inserted_meta, how='inner', on=info_inserted_meta.columns.to_list(), indicator=False)
    if ls_sectors == None:
        pass
    else:
        Table_Audit = Audit[Audit['Sector'].isin(list(set(ls_sectors)))]

    if ls_city == None:
        pass
    else:
        Table_Audit = Table_Audit[Table_Audit['City'].isin(list(set(ls_city)))]

    if ls_year == None:
        pass
    else:
        Table_Audit = Table_Audit[Table_Audit['YEAR'].isin(list(set(ls_year)))]

    if ls_quarter == None:
        pass
    else:
        Table_Audit = Table_Audit[Table_Audit['QUARTER'].isin(list(set(ls_quarter)))]

    return summ_fact, cross_check,Table_Audit



     