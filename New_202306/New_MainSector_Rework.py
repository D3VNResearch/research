from Connection import *

# Hàm lọc danh sách URL xuất hiện dựa trên sector
def getListImportFile(list_url):
    url = []
    for i in list_url:
        sector = i.split('/')[-1].split('.')[0].upper()
        if sector not in ('x'):
            url.append(i)
        else:
            pass
    return url

# Hàm tìm các thư mục chứa sector được chọn
def findFolderWithSector(list_folder, selected_sectors):
    # Tìm các folder chứa từ khóa
    selected_sector_folders = [folder for folder in list_folder if any(re.search(keyword, folder, re.IGNORECASE) for keyword in selected_sectors)]
    return selected_sector_folders

# Hàm tìm và lấy thông tin từ các thư mục
def findFolder(list_folder,selected_sectors , url_hub):
    # Tạo DataFrame rỗng để lưu thông tin
    df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
    for i in findFolderWithSector(list_folder, selected_sectors):
        if i.split('/')[8] in selected_sectors:
            df_summ_file = pd.concat([df_summ_file, ConnectSharePoint(url_hub).get_content_url(i)])
    # Tạo danh sách URL từ DataFrame
    list_url = df_summ_file['ServerRelativeUrl'].to_list()
    #History file
    df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
    df_summ_file = df_summ_file.reset_index(drop=True)
    return list_url , df_summ_file

# Hàm lọc và chọn sector dựa trên input
def GetSector(input_string, list_folder ,selected_sectors):  
    input_list = input_string.split(',')
    sector_list = []
    for i in list_folder:
        sector_list.append(i.split('/')[7])
    for input_sector in input_list:
        input_sector = input_sector.strip().lower()
        if input_sector == "-1":
            break

        matched_sectors = [sector for sector in sector_list if input_sector in sector.lower()]
        if matched_sectors:
            selected_sectors.extend(matched_sectors)
        else:
            print(f"Không tìm thấy sector chứa từ khóa '{input_sector}'.")
    return selected_sectors 
def GetProvince(input_string, list_folder ,selected_provinces):  
    input_list = input_string.split(',')
    province_list = []
    for i in list_folder:
        province_list.append(i.split('/')[8])
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
# def getFile(list_folder, selected_sectors, url_hub):
#     input_string = input("Nhập tên sector muốn lấy: ")
#     if input_string == 'APT':
#         input_string = 'APARTMENT'
#     elif input_string == 'SA':
#         input_string = 'SERVICED_APARTMENT'
#     print("Các sector đã chọn:")
#     for sector in GetSector(input_string, list_folder ,selected_sectors):
#         print(colored(sector,'yellow'))
#     list_url , df_summ_file = findFolder(list_folder, selected_sectors, url_hub)
#     list_url = getListImportFile(list_url)
#     return list_url, df_summ_file

def getFileMainSector(list_folder, selected_provinces, url_hub):
    url = []
    # Input provinces
    input_string = input("Nhập tên các tỉnh bạn muốn chọn, cách nhau bằng dấu phẩy (nhập -1 để thoát): ")
    # Input Năm và Quý
    choose_province = ''
    # In danh sách tỉnh đã chọn, năm và quý
    print("Các tỉnh đã chọn:")
    for province in GetProvince(input_string, list_folder ,selected_provinces):
        choose_province = province
        print(colored(province,'yellow'))
    list_url , df_summ_file = findFolder(list_folder, selected_provinces, url_hub)
    Subsector = input('Nhap Sector: ')
    for i in list_url:
        sector = i.split('/')[-1].split('_')[0].upper()
        if sector=='APARTMENT':
            sector='APT'
        else:
            pass
        if sector in (Subsector):#Chọn sector cần import: RETAIL, OFFICE, SA, HOTEL, APT, VLTH
            url.append(i)
        else:
            pass
    return url, df_summ_file


def insert_to_fresh_MainSector(file_url, data, cnt_str):

    engine = create_engine(cnt_str)
    #Test Connection
    try:
        result = engine.execute("SELECT 1")
        print("CONNECTION SUCESSFUL IN INSERT TO FRESH FUNCTION!")
    except Exception as e:
        print("CONNECTION FAILED IN INSERT TO FRESH FUNCTION!:",str(e))
    sector = file_url.split('/')[-1].split('_')[0].upper()
    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        file_name = file_url.split('/')[-1].split('.')[0]
        print(f"Insert {file_name} to Fresh.{sector}_New")
        if executemany:
            cursor.fast_executemany = True
    
    #-------------------------------------------------------
    print('Sector: ', sector)
    if sector=='APARTMENT' or sector=='APT':
        table_name='APT_New'
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
            data[i]=data[i].apply(convert_float).replace(r'\.0$','', regex=True) 
        data = data.replace({np.nan: None})
        data.to_excel('output.xlsx', index=False, engine='openpyxl')
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif sector=='VLTH':
        table_name='VLTH_New'
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
            data[i]=data[i].apply(convert_float).replace(r'\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')

    elif sector=='RETAIL':
        table_name='RETAIL_New'
        list_float_columns = ['Longtitude','Latitude','Service_Charge','Avg_Gross_Rent','Leased_Area',
                             'Vacant_Area','NLA','GFA','Number_Of_Floors','New_Supply'
                             ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace(r'\.0$','', regex=True)
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='OFFICE':
        table_name='OFFICE_New'
        list_float_columns=['Longtitude','Latitude', 'Gross_Office_Area', 'Efficiency', 'NLA',
                           'Leased_Area_End_Of_Q', 'Vacant_Area', 'Average_Price',
                           'Avg_Gross_Rent', 'Avg_Net_Rent','New_Supply'
                           ]
        for i in list_float_columns:
            if i not in data.columns: 
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace(r'\.0$','', regex=True)    
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')        
            
    elif sector=='HOTEL':
        table_name='HOTEL_New'
        data.rename(columns = {'Project_Type':'Sub_Project_Type'}, inplace = True)
        list_float_columns=['Longtitude','Latitude', 'Land_Area', 'Occupancy','Total_Supply',
                            'Leased_Rooms','Avg_Room_Price', 'Service_Charge', 'Quoted_Room_Rate', 'New_Supply'
                           ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace(r'\.0$','', regex=True) 
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        
    elif sector=='SERVICED_APARTMENT' or sector=='SA':
        table_name='Serviced_Apartment_New'
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
            data[i]=data[i].apply(convert_float).replace(r'\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
    elif sector=='Product_Mix' or sector =='PM' or sector == 'ProductMix':
        table_name='Product_Mix_New'
        list_float_columns=['LA','GFA', 'Balcony_GFA', 'Pool_GFA','Garden_GFA',
                            'Discount','Rent', 'Primary_Price_LA', 'Primary_Price_GFA', 'Secondary_Price_LA', 'Secondary_Price_GFA'
                        ]
        for i in list_float_columns:
            if i not in data.columns:
                data[i] = np.nan
            else:
                pass
            data[i]=data[i].apply(convert_float).replace(r'\.0$','', regex=True) 
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')

def check_duplicate_MainSector(data, column_name):
    unique_values = data['Date_Key'].astype(str) + data['Project_City_Name'].astype(str) + data['Project_District_Name'].astype(str)
    unique_values = unique_values.unique()
    duplicate_dfs = []

    for value in unique_values:
        filtered_data = data[data['Date_Key'].astype(str) + data['Project_City_Name'].astype(str) + data['Project_District_Name'].astype(str) == value]
        duplicates = filtered_data[filtered_data.duplicated(subset=[column_name], keep='last')]
        if not duplicates.empty:
            duplicate_dfs.append(duplicates)

    if duplicate_dfs:
        df_dup = pd.concat(duplicate_dfs)
    else:
        df_dup = pd.DataFrame()

    return data, df_dup  # Return both data and df_dup


def get_data_MainSector(relative_url, file_url):
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
    sector = file_url.split('/')[-1].split('.')[0].split('_')[0].upper()

    return data, file_name, sector

def Send_Email_MainSector(to_email,df_flat_ip,df_new_key_ip, cnt_str):
    list_df_flat = [df_flat_ip]
    list_df_new_key = [df_new_key_ip]
    engine = create_engine(cnt_str)

    #Email notify imported data
    df_flat_html, df_query_html = convert_df_to_html(type_html = 3, list_df = list_df_flat, type_sector = 2, cnxn = engine)
    run_email(type_sector = 'Main Sector', email_type = 3, user_email = to_email, df_flat_html = df_flat_html, df_query_html = df_query_html)

    #Email notify create new key
    df_new_key_html = convert_df_to_html(type_html = 4, list_df = list_df_new_key, cnxn = engine)
    if len(df_new_key_html) != 0:
        print(colored('Some new keys were created','yellow'))
        run_email(type_sector = 'Main Sector', email_type = 4, user_email = to_email, df_noti_html = df_new_key_html)    

def check_project_key_MainSector(file_url, data, sector, engine):
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

def check_new_key_MainSector(df_new_key = None, processed_data = None, sector = None):
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
                        , 'Project_City_Name','Project_Grade'
                       ]
        list_dim_columns = ['Sector', 'Project_City_Name', 'Date_Key', 'Project_District_Name'
                            , 'Project_Name', 'Sub_Project_Name', 'Project_Grade', 'Project_Status', 'Developer', 'Launch_Year'
                           ]
        processed_data = processed_data.rename(columns = {'Grade':'Project_Grade', 'Current_Status':'Project_Status'})
        
    else:
        print("check new key Main Sector : ", sector)
        return print(colored('Unknown sector from check_new_key_MainSector','yellow'))
    
    df = processed_data[list_dim_columns]

    for i in list_keys_on:
        df = df[df[i].notnull()].copy()
        df.loc[:,i] = df[i].apply(lambda x: x.title())
        df.loc[:,'Sector'] = df.loc[:, 'Sector'].apply(lambda x: x.upper())
    df_new_key = pd.concat([df_new_key, df], axis=0)
    for cols in df_new_key.columns:
        df_new_key[cols] = df_new_key[cols].fillna('NULL')
        df_new_key[cols] = df_new_key[cols].apply(lambda x: int(x) if isinstance(x, float) else x)
    
    return df_new_key

def tracking_flat_file_MainSector(data, file_url):
    sector = file_url.split('/')[-1].split('.')[0].split('_')[0].upper()

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
    else:
        print("tracking_flat_file_MainSector: ",sector)
        return print(colored('Unknown sector from tracking_flat_file_MainSector','yellow'))
    
    if sector not in ['IP', 'MACRO', 'FDI', 'INFRA', 'PM', 'TENANT']:
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

def get_project_key_MainSector(flag_key, processed_data, data, sector, engine):
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
    elif sector in ['PM']:
        list_keys=['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Grade','Project_Key']
        list_keys_on=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade']
        list_column_name_insert_project=['Sector','Project_Name','Sub_Project_Name','Project_Grade',
                                         'Project_City_Name','Project_District_Name','Date_Key','File_Date','Import_Date'
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
        print(colored("List new_key:",'yellow'),len(processed_data[list_column_name_insert_project]))
        # df_data = pd.DataFrame(processed_data)
        # # Read the existing Excel file into a DataFrame
        # existing_df = pd.read_excel("Data_parameter.xlsx", sheet_name="Sheet1")
        # # Concatenate the new data with the existing dat
        # final_df = pd.concat([existing_df, df_data], ignore_index=True)
        # # Save the concatenated DataFrame back to the Excel file
        # final_df.to_excel("Data_parameter.xlsx", index=False, sheet_name="Sheet1")
        print("Before get_project_key: ",len(data))
        #-------------------------------------------------------     
        if sector == 'RETAIL' or sector == 'VLTH':
            processed_data[list_column_name_insert_project] = processed_data[list_column_name_insert_project].drop_duplicates(subset=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Type'])
            df_data = pd.DataFrame(processed_data[list_column_name_insert_project])
            # Read the existing Excel file into a DataFrame
            # existing_df = pd.read_excel("Data_parameter.xlsx", sheet_name="Sheet1")
            # # Concatenate the new data with the existing dat
            # final_df = pd.concat([existing_df, df_data], ignore_index=True)
            # # Save the concatenated DataFrame back to the Excel file
            # final_df.to_excel("Data_parameter.xlsx", index=False, sheet_name="Sheet1")
            processed_data[list_column_name_insert_project].to_sql(f'{table_name}', 
                                                                engine, 
                                                                index=False, 
                                                                if_exists='append', 
                                                                schema='FRESH'
                                                          )
            delete_query = '''
                            DELETE FROM fresh.Main_Sector_Project
                            WHERE date_key IS NULL
                            '''
            engine.execute(text(delete_query))
        elif sector in ['HOTEL','SA','SERVICED_APARTMENT','OFFICE','APT','APARTMENT']:
            processed_data[list_column_name_insert_project] = processed_data[list_column_name_insert_project].drop_duplicates(subset=['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name','Project_Grade'])
            df_data = pd.DataFrame(processed_data[list_column_name_insert_project])
            # Read the existing Excel file into a DataFrame
            # existing_df = pd.read_excel("Data_parameter.xlsx", sheet_name="Sheet1")
            # # Concatenate the new data with the existing dat
            # final_df = pd.concat([existing_df, df_data], ignore_index=True)
            # Save the concatenated DataFrame back to the Excel file
            #final_df.to_excel("Data_parameter.xlsx", index=False, sheet_name="Sheet1")
            processed_data[list_column_name_insert_project].to_sql(f'{table_name}', 
                                                                engine, 
                                                                index=False, 
                                                                if_exists='append', 
                                                                schema='FRESH'
                                                          )
            delete_query = '''
                            DELETE FROM fresh.Main_Sector_Project
                            WHERE date_key IS NULL
                            '''
            engine.execute(text(delete_query))
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
        print("After get_project_key: ",len(data))
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


def importMainSector(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub):
        '''Prepare ingredients''' 
        columns_that_need_unidecode = ['Project_Name','Sub_Project_Name','Developer','Constructor'
                                    , 'Operator','Project_District_Name','Project_City_Name', 'Construction_Status'
                                    ]
        engine = create_engine(cnt_str)
        #Create empty df for checking dictionary
        df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
        #Create multi empty df for tracking autdit step
        name_sector = ['RETAIL', ' 5`SA', 'APT', 'VLTH', 'OFFICE', 'HOTEL']
        name_sector = [x.lower() for x in name_sector]
        for i in name_sector:
            globals()['df_temp_flat_{}'.format(i)] = pd.DataFrame([])
            globals()['df_flat_{}'.format(i)] = pd.DataFrame([]) 
            globals()['df_new_key_{}'.format(i)] = pd.DataFrame([])  
        #-------------------------------------------------------
        '''Get data''' 
        for file_url in tqdm(list_url):  
            data, file_name, sector = get_data_MainSector(url_hub, file_url)
            print('Sector: ', sector)
            print("\nRows ban đầu từ flat file:  ",len(data))
            #-------------------------------------------------------
            #data = check_date_key(file_url, data) #Check format date_key in flat file   
            data['Project_Name']= np.where(data['Project_Name'].isnull(), data['Sub_Project_Name'], data['Project_Name']) #Fill up project_name if its null
            #Check duplicate sub_name
            data, df_dup = check_duplicate_MainSector(data, 'Sub_Project_Name')
            print("Rows sau khi check duplicate: ",len(data))
            print("Số lượng rows bị duplicate: ",len(df_dup))
            if len(df_dup) != 0:
                print(colored('Check dupplicate sub_name', 'yellow'))
                df_noti_html = convert_df_to_html(type_html = 1, df = df_dup, type_sector = 1, cnxn = engine)
                to_email = ['pnguyenhaiduong@savills.com.vn', 'hcmcbi-intern07@savills.com.vn']
                run_email(type_sector = 'MAIN SECTOR', email_type = 1, user_email = to_email, df_noti_html = df_noti_html)
            else:       
                #-------------------------------------------------------
                '''Validation step'''
                #Remove unfortmated values
                #data = remove_unformated_character(type_sector = 1, sector = sector, file_name = file_name, data = data)
                data = remove_unformated_character(data)
                #Remove unicode characters
                for i in columns_that_need_unidecode:
                    if i not in data.columns:
                        data[i] = np.nan
                        data[i] = data[i].replace({np.nan: None})
                        print(colored('Column {} is added in {}','yellow').format(i, f'{file_name}'))
                    else:
                        pass   
                    data[i] = remove_unicode(data[i])
                # Check dictionary
                lst_dict = ['City', 'District', 'Status', 'Grade', 'Type','Developer']
                lst_cls = ['Project_City_Name', 'Project_District_Name', 'Project_Status', 'Grade', 'Sub_Project_Type','Developer']
                # lst_dict = ['City', 'District', 'Status']
                # lst_cls = ['Project_City_Name', 'Project_District_Name', 'Project_Status']
                #print('Before: \n',before_check)
                for i, j in zip(lst_cls, lst_dict):
                    data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
                #data.to_excel('output.xlsx', index=False, engine='openpyxl')
                # after_check = data['Sub_Project_Type'].value_counts()
                # print('After: \n',after_check)
                if len(df_dict) == 0:
                    print(colored('Validate succesfully','green'))
                    #-------------------------------------------------------
                    '''Import data process'''
                    #Check project key
                    data = Generate_Additional_Columns(data,df_summ_file,Hub,engine,file_url)
                    processed_data, flag_key = check_project_key_MainSector(file_url, data, sector, engine)
                    print("Rows sau khi check project key: ",len(data))
                    #Create tracking audit
                    if sector == 'RETAIL':
                        df_temp_flat_retail = pd.DataFrame()
                        df_temp_flat_retail = pd.concat([df_temp_flat_retail, data], axis=0)
                        df_flat_retail=tracking_flat_file_MainSector(df_temp_flat_retail, file_url)
                        if len(processed_data) != 0:
                            df_new_key_retail= pd.DataFrame()
                            df_new_key_retail = check_new_key_MainSector(df_new_key = df_new_key_retail, processed_data = processed_data, sector = sector)
                    elif sector == 'OFFICE':
                        df_temp_flat_office = pd.DataFrame()
                        df_temp_flat_office = pd.concat([df_temp_flat_office, data], axis=0)
                        df_flat_office = tracking_flat_file_MainSector(df_temp_flat_office, file_url)
                        if len(processed_data) != 0:
                            df_new_key_office= pd.DataFrame()
                            df_new_key_office = check_new_key_MainSector(df_new_key = df_new_key_office, processed_data = processed_data, sector = sector)
                    elif sector == 'HOTEL':
                        df_temp_flat_hotel = pd.DataFrame()
                        df_temp_flat_hotel = pd.concat([df_temp_flat_hotel, data], axis=0)
                        df_flat_hotel = tracking_flat_file_MainSector(df_temp_flat_hotel, file_url)
                        if len(processed_data) != 0:
                            df_new_key_hotel= pd.DataFrame()
                            df_new_key_hotel = check_new_key_MainSector(df_new_key = df_new_key_hotel, processed_data = processed_data, sector = sector)
                    elif sector == 'SA' or sector=='SERVICED_APARTMENT':
                        df_temp_flat_sa = pd.DataFrame()
                        df_temp_flat_sa = pd.concat([df_temp_flat_sa, data], axis=0)
                        df_flat_sa = tracking_flat_file_MainSector(df_temp_flat_sa, file_url)
                        if len(processed_data) != 0:
                            df_new_key_sa= pd.DataFrame()
                            df_new_key_sa = check_new_key_MainSector(df_new_key = df_new_key_sa, processed_data = processed_data, sector = sector)
                    elif sector == 'APT' or sector=='APARTMENT':
                        df_temp_flat_apt = pd.DataFrame()
                        df_temp_flat_apt = pd.concat([df_temp_flat_apt, data], axis=0)
                        df_flat_apt = tracking_flat_file_MainSector(df_temp_flat_apt, file_url)
                        if len(processed_data) != 0:
                            df_new_key_apt= pd.DataFrame()
                            df_new_key_apt = check_new_key_MainSector(df_new_key = df_new_key_apt, processed_data = processed_data, sector = sector)
                    
                    elif sector == 'VLTH':
                        df_temp_flat_vlth = pd.DataFrame()
                        df_temp_flat_vlth = pd.concat([df_temp_flat_vlth, data], axis=0)
                        df_flat_vlth = tracking_flat_file_MainSector(df_temp_flat_vlth, file_url)
                        if len(processed_data) != 0:
                            df_new_key_vlth= pd.DataFrame()
                            df_new_key_vlth = check_new_key_MainSector(df_new_key = df_new_key_vlth, processed_data = processed_data, sector = sector) 
                    else:
                        pass
                    #Get key and generate new key (if needed)
                    data = get_project_key_MainSector(flag_key, processed_data, data, sector, engine)
                    print("Rows sau khi qua hàm get_project_key_MainSector: ",len(data))
                    data['File_Name'] = file_name.replace('.xlsx', '').replace('.csv', '')
                    try:
                        result = insert_to_fresh_MainSector(file_url, data, cnt_str)
                        #data.to_excel('output.xlsx', index=False, engine='openpyxl')
                        print(colored("insert_to_fresh SUCESSFUL!",'green'))
                    except Exception as e:
                        print(colored("insert_to_fresh FAILED:",str(e),'red'))
                else:
                    # print(df_dict)
                    # df_noti_html = convert_df_to_html(type_html = 2, df = df_dict, type_sector = 2, cnxn = engine)
                    # run_email(type_sector = 'Main Sector', email_type = 2, user_email = to_email, df_noti_html = df_noti_html)
                    # print(colored('Validate failed','red'))

                    # Save the concatenated DataFrame back to the Excel file
                    df_dict.to_excel("Missing_Dictionary.xlsx", index=False, sheet_name="Sheet1")
                    pass

def main():
    cnt_str = ConnectAzureSQLServer()
    engine = create_engine(cnt_str)
    # set to_email
    selected_sectors = [] 
    while(True):
        url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/Main sector/Flat file/Province'
        Hub= ConnectSharePoint(url_hub)
        sp_object = url_hub.split('/')[2].replace('-','')
        list_folder = Hub.get_content_url(url_hub, return_list_folder=True)
        list_url, df_summ_file = getFileMainSector(list_folder, selected_sectors, url_hub)
        selected_sectors.clear()
        importMainSector(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)

if __name__ == "__main__":
    main() 