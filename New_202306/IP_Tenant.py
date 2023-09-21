from Connection import *
def getListImportFile(list_url):
    url = []
    for i in list_url:
        sector = i.split('/')[-1].split('_')[0].upper()
        if sector not in ('x'):
            url.append(i)
        else:
            pass
    return url
def findFolderWithProvince(list_folder, selected_provinces):
    # Tìm các folder chứa từ khóa
    selected_province_folders = [folder for folder in list_folder if any(re.search(keyword, folder, re.IGNORECASE) for keyword in selected_provinces)]
    return selected_province_folders

def findFolder(list_folder,selected_provinces , url_hub):
    df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
    for i in findFolderWithProvince(list_folder, selected_provinces):
        if i.split('/')[8] in selected_provinces:
            df_summ_file = pd.concat([df_summ_file, ConnectSharePoint(url_hub).get_content_url(i)])
    list_url = df_summ_file['ServerRelativeUrl'].to_list()
    #History file
    df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
    df_summ_file = df_summ_file.reset_index(drop=True)
    return list_url , df_summ_file
    
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

def getFile(list_folder, selected_provinces, url_hub):
    # Input provinces
    input_string = input("Nhập tên các tỉnh bạn muốn chọn, cách nhau bằng dấu phẩy (nhập -1 để thoát): ")
    choose_province = ''
    # In danh sách tỉnh đã chọn, năm và quý
    print("Các tỉnh đã chọn:")
    for province in GetProvince(input_string, list_folder ,selected_provinces):
        choose_province = province
        print(colored(province,'yellow'))
    list_url , df_summ_file = findFolder(list_folder, selected_provinces, url_hub)
    list_url = getListImportFile(list_url)
    return list_url, df_summ_file

def insert_to_fresh_IP_Tenant(file_url, data, cnt_str):
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
    sector = file_url.split('/')[-1].split('_')[1].upper()
    # ------------------------------------------------------- 
    if sector == 'TENANT':
        table_name='IP_Tenant_New'
        for i in data.columns:
            if i == 'Industry_lv_1':
                data = data.rename(columns = {i:'Industry_Lv1'})
            elif i == 'Industry_lv_2':
                data = data.rename(columns = {i:'Industry_Lv2'})
            elif i == 'Industry_lv_3':
                data = data.rename(columns = {i:'Industry_Lv3'})
            elif i == 'Industry_lv_4':
                data = data.rename(columns = {i:'Industry_Lv4'})
            elif i == 'Industry_lv_5':
                data = data.rename(columns = {i:'Industry_Lv5'})
            else:
                pass
        list_float_columns = ['Acquired_Area','Tenant_Longtitude','Tenant_Latitude']
        for i in list_float_columns:
            data[i]=data[i].apply(convert_float).replace('\.0$','', regex=True)   
        data = data.replace({np.nan: None})
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')

def check_duplicate_IP_Tenant(data, column_name):
    unique_values_Date_Key = data['Date_Key'].unique()
    unique_values_SPN = data['Sub_Project_Name'].unique()
    duplicate_dfs = []

    for value in unique_values_Date_Key:
        for value in unique_values_SPN:
            filtered_data_Date_Key = data[data['Date_Key'] == value]
            duplicates_DK = filtered_data_Date_Key[filtered_data_Date_Key.duplicated(subset=[column_name], keep='last')]
            filtered_data_SPN = data[data['Sub_Project_Name'] == value]
            duplicates_SPN = filtered_data_SPN[filtered_data_SPN.duplicated(subset=[column_name], keep='last')]
            if (not duplicates_DK.empty) and (not duplicates_SPN.empty) :
                duplicate_dfs.append(duplicates_DK)
                duplicate_dfs.append(duplicates_SPN)

    # for value in unique_values_SPN:
    #     filtered_data_SPN = data[data['Sub_Project_Name'] == value]
    #     duplicates = filtered_data_SPN[filtered_data_SPN.duplicated(subset=[column_name], keep='last')]
    #     if not duplicates.empty:
    #         duplicate_dfs.append(duplicates)

    if duplicate_dfs:
        df_dup = pd.concat(duplicate_dfs)
    else:
        df_dup = pd.DataFrame()

    return data, df_dup  # Return both data and df_dup

def Send_Email_IP(to_email,df_flat_ip,df_new_key_ip, cnt_str):
    list_df_flat = [df_flat_ip]
    list_df_new_key = [df_new_key_ip]
    engine = create_engine(cnt_str)

    #Email notify imported data
    df_flat_html, df_query_html = convert_df_to_html(type_html = 3, list_df = list_df_flat, type_sector = 2, cnxn = engine)
    run_email(type_sector = 'IP', email_type = 3, user_email = to_email, df_flat_html = df_flat_html, df_query_html = df_query_html)

    #Email notify create new key
    df_new_key_html = convert_df_to_html(type_html = 4, list_df = list_df_new_key, cnxn = engine)
    if len(df_new_key_html) != 0:
        print(colored('Some new keys were created','yellow'))
        run_email(type_sector = 'IP', email_type = 4, user_email = to_email, df_noti_html = df_new_key_html)    

def check_dictionary_IP_Tenant(df_dict, file_name, data, column_name, parameter, sector, cnxn, sp_object):
    lower_function = lambda x: x.strip().lower() if isinstance(x, str) else x
    if parameter in ['City', 'District', 'Status', 'Indicator'
                     , 'Country', 'Industry'
                    ]:
        raw_parameter = pd.read_sql(f'select * from GENERAL.{parameter}_Dictionary',cnxn)
        raw_parameter[f'Raw_{parameter}'] = raw_parameter[f'Raw_{parameter}'].apply(lower_function)
    elif parameter in ['Type', 'Sub_Type']:
        if sector in [ 'TENANT']:
            raw_parameter = pd.read_sql(f"select * from GENERAL.Project_{parameter}_Dictionary WHERE Sector = '{sector}' ",cnxn)
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
    # print(data[['Grade','Convert_Grade']])
    if parameter == 'Grade' or parameter == 'Type' or parameter == 'Country' :
        temp = data[[f'{column_name}',f'Convert_{parameter}']].dropna()
        parameter_not_in_dict = temp[f'{column_name}'][temp[f'Convert_{parameter}'].isnull()]        
    else:
        parameter_not_in_dict = data[f'{column_name}'][data[f'Convert_{parameter}'].isnull()]
    print(parameter_not_in_dict)
   # print(parameter_not_in_dict_temp)
    # print(data['Grade'])
    #print(data)
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

def get_data_IP_Tenant(relative_url, file_url):
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
    # sector = file_url.split('/')[-1].split('_')[0].upper()
    sector = file_url.split('/')[-1].split('_')[1].upper()
    if sector == 'PM':
        sector = file_url.split('/')[-1].split('_')[1].upper()

    if sector == 'APT':
        sector = 'APARTMENT'
    elif sector == 'SA':
        sector = 'SERVICED_APARTMENT'
    return data, file_name, sector


def importIP_Tenant(list_folder, url_hub, list_url, to_email,cnt_str, sp_object,df_summ_file, Hub):
        columns_that_need_unidecode=['Project_Name', 'Sub_Project_Name', 'Tenant_Name_ENG'
                             , 'City', 'District', 'Tenant_Name_VN', 'Nationality', 'Slot', 'Industry_lv_1'
                             , 'Industry_lv_2', 'Industry_lv_3', 'Industry_lv_4', 'Industry_lv_5']
        engine = create_engine(cnt_str)
        #Create empty df for checking dictionary
        df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
        #Create multi empty df for tracking autdit step
        name_sector = ['IP']
        name_sector = [x.lower() for x in name_sector]
        for i in name_sector:
            globals()['df_temp_flat_{}'.format(i)] = pd.DataFrame([])
            globals()['df_flat_{}'.format(i)] = pd.DataFrame([])
            globals()['df_new_key_{}'.format(i)] = pd.DataFrame([])  
        #-------------------------------------------------------
        '''Get data''' 
        print('Start Get Data...')
        for file_url in tqdm(list_url):  
            data, file_name, sector = get_data_IP_Tenant(url_hub, file_url)
            #-------------------------------------------------------
            #data = check_date_key(file_url, data)#Check format date_key in flat file   
            data['Project_Name']= np.where(data['Project_Name'].isnull(), data['Sub_Project_Name'], data['Project_Name'])#Fill up project_name if its null
            data['Project_Sub_Type']= np.where(data['Project_Sub_Type'].isnull(), data['Project_Type'], data['Project_Sub_Type'])#Fill up Sub_Type if its null
            
            #Check duplicate sub_name
            print('Start Check duplicate sub_name...')
            data, df_dup = check_duplicate_IP_Tenant(data, 'Tenant_Name_VN')
            if len(df_dup) != 0:
                print(colored('Check duplicate sub_name', 'yellow'))
                print('\n', df_dup)
            else:
                df_flat_ip = pd.DataFrame()   
                df_new_key_ip= pd.DataFrame()
                #-------------------------------------------------------
                
                '''Validation step'''
                print('Start validation step...')
                #Remove unfortmated values
                data = remove_unformated_character(data)
                #Remove unicode characters
                for i in columns_that_need_unidecode:
                    data[i] = remove_unicode(data[i])
                #Check dictionary
                print('Start Check dictionary...')
                # lst_dict = ['City', 'District', 'Status', 'Sub_Type', 'Type']
                # lst_cls = ['City', 'District', 'Tenant_Status', 'Project_Sub_Type', 'Project_Type']
                lst_dict = ['City', 'District', 'Type', 'Country']
                lst_cls = ['City', 'District', 'Project_Type', 'Nationality']
                for i, j in zip(lst_cls, lst_dict):
                    print(f'Start checking dictionary of [{i}:{j}]...')
                    data, df_dict = check_dictionary_IP_Tenant(df_dict, file_name, data, i, j, sector, engine, sp_object)
                print(f'End Check dictionary with df_dict = {len(df_dict)} ...')
                
                if len(df_dict) == 0:
                    print(colored('Validate succesfully','green'))
                    #-------------------------------------------------------
                    '''Import data process'''
                    #Check project key
                    data = Generate_Additional_Columns(data,df_summ_file, Hub,engine,file_url)
                    data['File_Date'] = pd.to_datetime(data['File_Date']).dt.date
                    ## Tam tat check project key
                    # /*
                    # processed_data, flag_key = check_project_key(file_url, data, sector, engine)
                    # df_temp_flat_ip = pd.DataFrame() 
                    # df_temp_flat_ip = pd.concat([df_temp_flat_ip, data], axis=0)
                    # df_flat_ip = tracking_flat_file(df_temp_flat_ip, file_url)
                    # if len(processed_data) != 0:
                        
                    #     df_new_key_ip = check_new_key(df_new_key = df_new_key_ip, processed_data = processed_data, sector = sector)
                    # #Get key and generate new key (if needed)
                    # data = get_project_key(flag_key, processed_data, data, sector, engine)
                    # */
                    #insert_to_fresh(file_url, data, cnt_str)
                    #Send_Email_IP(to_email, df_flat_ip, df_new_key_ip, cnt_str)
                    #Test insert_to_fresh
                    for i in data.columns:
                        if i == 'IP_Tenant_Key':
                            data.drop('IP_Tenant_Key', axis=1, inplace=True)
                            data.drop('Is_New', axis=1, inplace=True)
                    try:
                        result = insert_to_fresh_IP_Tenant(file_url, data, cnt_str)
                        print(colored("insert_to_fresh SUCESSFUL!",'green'))
                    except Exception as e:
                        print('er')
                        print(colored("insert_to_fresh FAILED:",str(e),'red'))
                    
                else:
                    print(df_dict)
                    print(colored('Validate failed','red'))
                    pass


def main():
    cnt_str = ConnectAzureSQLServer()
    engine = create_engine(cnt_str)
    # set to_email
    to_email = ['hcmcbi-intern04@savills.com.vn']
    selected_provinces = [] 
    while(True):
        Sector= 'IP Tenant'
        url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/{Sector}/IP tenant/Flat file'
        Hub= ConnectSharePoint(url_hub)
        sp_object = url_hub.split('/')[2].replace('-','')
        list_folder = Hub.get_content_url(url_hub, return_list_folder=True)
        if (Sector == 'IP Tenant'):
            list_url, df_summ_file = getFile(list_folder, selected_provinces, url_hub)
            selected_provinces.clear()
            importIP_Tenant(list_folder, url_hub, list_url, to_email, cnt_str, sp_object,df_summ_file, Hub)


if __name__ == "__main__":
    main()