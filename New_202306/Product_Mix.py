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
        if i.split('/')[7] in selected_sectors:
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


def getFileProductMix(list_folder, selected_provinces, url_hub):
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


def insert_to_fresh_Product_Mix(file_url, data, cnt_str):
    
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
        table_name='Product_Mix_New'
        list_float_columns = ['LA', 'GFA', 'Balcony_GFA', 'Pool_GFA', 'Garden_GFA', 
                             'Discount', 'Incentive', 'Rent', 'Primary_Price_LA', 'Primary_Price_GFA', 
                              'Secondary_Price_LA', 'Secondary_Price_GFA'
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
        table_name='Product_Mix_New'
        list_float_columns = ['LA', 'GFA', 'Balcony_GFA', 'Pool_GFA', 'Garden_GFA', 
                             'Discount', 'Incentive', 'Rent', 'Primary_Price_LA', 'Primary_Price_GFA', 
                              'Secondary_Price_LA', 'Secondary_Price_GFA']
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


def get_data_PM(relative_url, file_url):
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

def Send_Email_PM(to_email,df_flat_ip,df_new_key_ip, cnt_str):
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

def check_project_key_PM(file_url, data, sector, engine):
    flag_key=0
    if 'Project_Key' not in data.columns:
        data['Project_Key'] = np.nan
        data['Project_Key'] = data['Project_Key'].replace({np.nan: None})
    else:
        pass
    
    if sector in ['VLTH', 'RETAIL']:
        list_keys = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Key']
        list_keys_on =['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name']

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

def check_new_key_PM(df_new_key = None, processed_data = None, sector = None):
    if sector in ['VLTH', 'RETAIL']:
        list_keys_on = ['Sector', 'Sub_Project_Name','Project_District_Name'
                        ,'Project_City_Name'
                       ]
        list_dim_columns = ['Sector', 'Project_City_Name', 'Date_Key', 'Project_District_Name'
                            , 'Project_Name', 'Sub_Project_Name'
                           ]
    elif sector in ['OFFICE','APT','APARTMENT','HOTEL','SA','SERVICED_APARTMENT']:
        list_keys_on = ['Sector', 'Sub_Project_Name','Project_District_Name'
                        , 'Project_City_Name','Project_Grade'
                       ]
        list_dim_columns = ['Sector', 'Project_City_Name', 'Date_Key', 'Project_District_Name'
                            , 'Project_Name', 'Sub_Project_Name', 'Project_Grade'
                           ]
        processed_data = processed_data.rename(columns = {'Grade':'Project_Grade'})
        
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

def tracking_flat_file_PM(data, file_url):
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
        list_columns_sum =[]
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'File_Name'
                      ]
    elif sector == 'VLTH':
        list_columns_sum =[]
        re_position = ['Sector', 'Project_City_Name', 'Date_Key', 'File_Name'
                      ]
    else:
        print("tracking_flat_file_MainSector: ",sector)
        return print(colored('Unknown sector from tracking_flat_file_MainSector','yellow'))
    
    if sector not in ['IP', 'MACRO', 'FDI', 'INFRA', 'TENANT']:
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

def get_project_key_PM(flag_key, processed_data, data, sector, engine):
    if sector in ['VLTH', 'RETAIL']:
        list_keys = ['Sector', 'Sub_Project_Name', 'Project_District_Name','Project_City_Name','Project_Key']
        list_keys_on =['Sector', 'Sub_Project_Name','Project_District_Name','Project_City_Name']
        # if sector == 'VLTH':
        #     if 'Brand' in data.columns:
        #         list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
        #                                           ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
        #                                           ,'Project_District_Name','Developer','Date_Key','File_Date'
        #                                           ,'Import_Date', 'Brand'
        #                                          ]
        #     else:
        #         list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
        #                                           ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
        #                                           ,'Project_District_Name','Developer','Date_Key','File_Date'
        #                                           ,'Import_Date'
        #                                          ]
        # else:
        #     list_column_name_insert_project =['Sector','Project_Name','Sub_Project_Name','Latitude','Longtitude'
        #                                       ,'Project_Type','Current_Status','Project_Phase','Project_City_Name'
        #                                       ,'Project_District_Name','Developer','Date_Key','File_Date'
        #                                       ,'Import_Date'
        #                                      ]
            
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
        processed_data = processed_data.rename(columns={'Grade':'Project_Grade', 'Project_Status':'Current_Status'})    
        for i in list_keys_on:
            processed_data[i]=processed_data[i].str.title()
        if sector != 'IP':
            processed_data['Sector'] = processed_data['Sector'].str.upper()
        else:
            pass
        # df_data = pd.DataFrame(processed_data)
        # # Read the existing Excel file into a DataFrame
        # existing_df = pd.read_excel("Data_parameter.xlsx", sheet_name="Sheet1")
        # # Concatenate the new data with the existing dat
        # final_df = pd.concat([existing_df, df_data], ignore_index=True)
        # # Save the concatenated DataFrame back to the Excel file
        # final_df.to_excel("Data_parameter.xlsx", index=False, sheet_name="Sheet1")
        #-------------------------------------------------------     

        if sector in ['OFFICE','APT','APARTMENT'
                        ,'HOTEL','SA','SERVICED_APARTMENT'
                       ]:
            data = data.rename(columns = {'Grade':'Project_Grade'})    
        #-------------------------------------------------------    
        df_query = pd.read_sql(f"select * from DW.{table_name}",engine)
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
        elif sector in ['Product_Mix', 'PM']:
            data.rename(columns = {'Project_Key_y':'Project_Key'}, inplace = True)
            data['Sector']=data['Sector'].str.upper()
            data['File_Date'] = pd.to_datetime(data['File_Date']).dt.date
        else:
            data.rename(columns = {'Project_Key_y':'Project_Key'}, inplace = True)
            data['File_Date'] = pd.to_datetime(data['File_Date']).dt.date
        print('New key: ', data)
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

        df_query = pd.read_sql(f"select * from DW.{table_name}",engine)
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

def check_dictionary_PM(df_dict, file_name, data, column_name, parameter, sector, cnxn, sp_object):
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
            if sector == 'APT':
                sector = 'APARTMENT'
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
    elif parameter in ['Developer']:
        if sector in ['RETAIL', 'VLTH', 'APARTMENT'
                      , 'APT', 'HOTEL', 'SA', 'SERVICED_APARTMENT', 'OFFICE']:
            raw_parameter = pd.read_sql(f"select * from GENERAL.{parameter}_Dictionary WHERE Sector = 'Main Sector' ",cnxn)
            raw_parameter[f'Raw_{parameter}'] = raw_parameter[f'Raw_{parameter}'].apply(lower_function)
        elif sector in ['IP']:
            raw_parameter = pd.read_sql(f"select * from GENERAL.{parameter}_Dictionary WHERE Sector = 'Ip' ",cnxn)
            raw_parameter[f'Raw_{parameter}'] = raw_parameter[f'Raw_{parameter}'].apply(lower_function)
        else:
            return data, df_dict
    else:
        print(colored('Unknown parameter in check dictionary section','yellow'))
        return data, df_dict 
    #print('Before: ',data['Grade'])
    data[f'{column_name}']= data[f'{column_name}'].apply(lower_function)

    # data[f'Convert_{parameter}'] = pd.merge(data, raw_parameter
    #                                         , how='left'
    #                                         , left_on=f'{column_name}'
    #                                         , right_on=f'Raw_{parameter}')[f'Cleaned_{parameter}']
    # Hieu update - 31-05-2023
    #get_cleaned_type = lambda x: raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x,f'Cleaned_{parameter}'].values[0] #fix lỗi duplicate của hàm merge trên\
    get_cleaned_type = lambda x: raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x, f'Cleaned_{parameter}'].values[0] if len(raw_parameter.loc[raw_parameter[f'Raw_{parameter}'] == x, f'Cleaned_{parameter}']) > 0 else None
    data[f'Convert_{parameter}'] = data[f'{column_name}'].map(get_cleaned_type)
    ## End - Hieu update - 31-05-2023
    #print(data[['Grade','Convert_Grade']])
    print('parameter: ', parameter)
    if sector in ['IP']:
        data['Developer_Name']= data['Developer_Name'].fillna('No Information')
    if sector in ['FDI']:
        data['District']= data['District'].fillna('No Information')
    # if not data['Developer'].isnull().all():
    #     data['Developer'] = data['Developer'].fillna('No Information')

    if parameter == 'Grade' or parameter == 'Type' or parameter == 'Investment_Form':
        temp = data[[f'{column_name}',f'Convert_{parameter}']].dropna()
        parameter_not_in_dict = temp[f'{column_name}'][temp[f'Convert_{parameter}'].isnull()]      
    else:
        parameter_not_in_dict = data[f'{column_name}'][data[f'Convert_{parameter}'].isnull()]

    print(parameter_not_in_dict)
    if sector in ['PM']:
        data['Grade']= data['Grade'].fillna('Unrated')
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
def importProduct_Mix(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub):
        '''Prepare ingredients''' 
        columns_that_need_unidecode = ['Project_Name','Sub_Project_Name','Project_District_Name','Project_City_Name']
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
            data, file_name, sector = get_data_PM( url_hub, file_url)
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
                lst_dict = ['City', 'District', 'Grade']
                lst_cls = ['Project_City_Name', 'Project_District_Name', 'Grade']
                # lst_dict = ['City', 'District', 'Status']
                # lst_cls = ['Project_City_Name', 'Project_District_Name', 'Project_Status']
                #print('Before: \n',before_check)
                for i, j in zip(lst_cls, lst_dict):
                    data, df_dict = check_dictionary_PM(df_dict, file_name, data, i, j, sector, engine, sp_object)
                #data.to_excel('output.xlsx', index=False, engine='openpyxl')
                # after_check = data['Sub_Project_Type'].value_counts()
                # print('After: \n',after_check)
                if len(df_dict) == 0:
                    print(colored('Validate succesfully','green'))
                    #-------------------------------------------------------
                    '''Import data process'''
                    #Check project key
                    data = Generate_Additional_Columns(data,df_summ_file,Hub,engine,file_url)
                    processed_data, flag_key = check_project_key_PM(file_url, data, sector, engine)
                    print("Rows sau khi check project key: ",len(data))
                    #Create tracking audit
                    if sector == 'APT' or sector=='APARTMENT':
                        df_temp_flat_apt = pd.DataFrame()
                        df_temp_flat_apt = pd.concat([df_temp_flat_apt, data], axis=0)
                        df_flat_apt = tracking_flat_file_PM(df_temp_flat_apt, file_url)
                        if len(processed_data) != 0:
                            df_new_key_apt= pd.DataFrame()
                            df_new_key_apt = check_new_key_PM(df_new_key = df_new_key_apt, processed_data = processed_data, sector = sector)
                    
                    elif sector == 'VLTH':
                        df_temp_flat_vlth = pd.DataFrame()
                        df_temp_flat_vlth = pd.concat([df_temp_flat_vlth, data], axis=0)
                        df_flat_vlth = tracking_flat_file_PM(df_temp_flat_vlth, file_url)
                        if len(processed_data) != 0:
                            df_new_key_vlth= pd.DataFrame()
                            df_new_key_vlth = check_new_key_PM(df_new_key = df_new_key_vlth, processed_data = processed_data, sector = sector) 
                    else:
                        pass
                    #Get key and generate new key (if needed)
                    data = get_project_key_PM(flag_key, processed_data, data, sector, engine)
                    print("Rows sau khi qua hàm get_project_key_MainSector: ",len(data))
                    data['File_Name'] = file_name.replace('.xlsx', '').replace('.csv', '')
                    try:
                        result = insert_to_fresh_Product_Mix(file_url, data, cnt_str)
                        #data.to_excel('output.xlsx', index=False, engine='openpyxl')
                        data.to_excel("data.xlsx", index=False, sheet_name="Sheet1")
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
        url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/Product mix/Flat file'
        Hub= ConnectSharePoint(url_hub)
        sp_object = url_hub.split('/')[2].replace('-','')
        list_folder = Hub.get_content_url(url_hub, return_list_folder=True)
        list_url, df_summ_file = getFileProductMix(list_folder, selected_sectors, url_hub)
        selected_sectors.clear()
        importProduct_Mix(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)
if __name__ == "__main__":
    main() 