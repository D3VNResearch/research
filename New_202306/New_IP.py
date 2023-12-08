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
        if i.split('/')[7] in selected_provinces:
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

def insert_to_fresh_IP(file_url, data, cnt_str):
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
    if sector == 'IP':
        table_name = 'Industrial_Park_New'
        for i in data.columns:
            if i == 'Date_Import' or i == 'Date_import':
                data = data.rename(columns = {i:'Input_Date'})
            else:
                pass
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')


def check_duplicate_IP(data, column_name):
    unique_values = data['Date_Key'].unique()
    duplicate_dfs = []

    for value in unique_values:
        filtered_data = data[data['Date_Key'] == value]
        duplicates = filtered_data[filtered_data.duplicated(subset=[column_name], keep='last')]
        if not duplicates.empty:
            duplicate_dfs.append(duplicates)

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

def importIP(list_folder, url_hub, list_url, to_email,cnt_str, sp_object,df_summ_file, Hub):
        columns_that_need_unidecode=['Project_Name', 'Sub_Project_Name', 'Developer_Name'
                             , 'City', 'District', 'Target_Industry', 'Developer_Nationality']
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
            print(file_url)
            data, file_name, sector = get_data(url_hub, file_url)
            #-------------------------------------------------------
            #data = check_date_key(file_url, data)#Check format date_key in flat file   
            data['Project_Name']= np.where(data['Project_Name'].isnull(), data['Sub_Project_Name'], data['Project_Name'])#Fill up project_name if its null
            data['Project_Sub_Type']= np.where(data['Project_Sub_Type'].isnull(), data['Project_Type'], data['Project_Sub_Type'])#Fill up Sub_Type if its null
            
            #Check duplicate sub_name
            print('Start Check duplicate sub_name...')
            data, df_dup = check_duplicate_IP(data, 'Sub_Project_Name')
            if len(df_dup) != 0:
                print(colored('Check duplicate sub_name', 'yellow'))
                df_noti_html = convert_df_to_html(type_html = 1, df = df_dup, type_sector = 2, cnxn = engine)
                run_email(type_sector = 'IP', email_type = 1, user_email = to_email, df_noti_html = df_noti_html)
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
                lst_dict = ['City', 'District', 'Status', 'Sub_Type', 'Type']
                lst_cls = ['City', 'District', 'Status', 'Project_Sub_Type', 'Project_Type']
                for i, j in zip(lst_cls, lst_dict):
                    print(f'Start checking dictionary of [{i}:{j}]...')
                    data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
                print(f'End Check dictionary with df_dict = {len(df_dict)} ...')
                
                if len(df_dict) == 0:
                    print(colored('Validate succesfully','green'))
                    #-------------------------------------------------------
                    '''Import data process'''
                    #Check project key
                    data = Generate_Additional_Columns(data,df_summ_file, Hub,engine,file_url)
                    processed_data, flag_key = check_project_key(file_url, data, sector, engine)
                    df_temp_flat_ip = pd.DataFrame() 
                    df_temp_flat_ip = pd.concat([df_temp_flat_ip, data], axis=0)
                    df_flat_ip = tracking_flat_file(df_temp_flat_ip, file_url)
                    if len(processed_data) != 0: 
                        df_new_key_ip = check_new_key(df_new_key = df_new_key_ip, processed_data = processed_data, sector = sector)
                        df_new_key_ip = df_new_key_ip.drop_duplicates(subset=['Sub_Project_Name', 'District', 'City', 'Project_Sub_Type'])
                    combine_key=data['City']+data['District']+data['Sub_Project_Name']+data['Project_Sub_Type']
                    list_combine_key=[]
                    list_combine_key.append(combine_key)
                    count_num = list_combine_key.count(combine_key)
                    if count_num> 1:
                        print(combine_key)
                    #Get key and generate new key (if needed)
                    data = get_project_key(flag_key, processed_data, data, sector, engine)
                    #insert_to_fresh(file_url, data, cnt_str)
                    Send_Email_IP(to_email, df_flat_ip, df_new_key_ip, cnt_str)
                    #Test insert_to_fresh
                    try:
                        result = insert_to_fresh_IP(file_url, data, cnt_str)
                        print(colored("insert_to_fresh SUCESSFUL!",'green'))
                    except Exception as e:
                        print('er')
                        print(colored("insert_to_fresh FAILED:",str(e),'red'))
                    
                else:
                    print(df_dict)
                    df_noti_html = convert_df_to_html(type_html = 2, df = df_dict, type_sector = 2, cnxn = engine)
                    run_email(type_sector = 'IP', email_type = 2, user_email = to_email, df_noti_html = df_noti_html)
                    print(colored('Validate failed','red'))
                    pass


def main():
    cnt_str = ConnectAzureSQLServer()
    engine = create_engine(cnt_str)
    # set to_email
    to_email = ['hcmcbi-intern04@savills.com.vn']
    selected_provinces = [] 
    while(True):
        Sector= 'IP'
        url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/{Sector}/Flat file'
        Hub= ConnectSharePoint(url_hub)
        sp_object = url_hub.split('/')[2].replace('-','')
        list_folder = Hub.get_content_url(url_hub, return_list_folder=True)
        if (Sector == 'IP'):
            list_url, df_summ_file = getFile(list_folder, selected_provinces, url_hub)
            selected_provinces.clear()
            importIP(list_folder, url_hub, list_url, to_email, cnt_str, sp_object,df_summ_file, Hub)


if __name__ == "__main__":
    main()