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

def insert_to_fresh_Macro(file_url, data, cnt_str):
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
    if sector == 'MACRO':
        table_name = 'MACRO_New'
        for i in data.columns:
            if i == 'Date_Import' or i == 'Date_import':
                data = data.rename(columns = {i:'Input_Date'})
            else:
                pass
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='Fresh')
        



def importMacroEconomic(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub):
    '''Prepare ingredients'''
    columns_that_need_unidecode=['City', 'District', 'Indicator', 'Attribute_lv1', 'Attribute_lv2', 
                                'Indicator_Unit', 'Source'
                                ]
    #Create empty df for checking dictionary
    df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
    df_temp_flat_macro = pd.DataFrame()
    df_flat_macro = pd.DataFrame()
    engine = create_engine(cnt_str)
    #-------------------------------------------------------
    '''Get data''' 
    for file_url in tqdm(list_url):
        print('Start get data and import ', file_url)
        print()
        data, file_name, sector = get_data(url_hub, file_url)
        #-------------------------------------------------------
        # data = check_date_key(file_url, data)#Check format date_key in flat file
        #-------------------------------------------------------
        '''Validation step'''
        #Remove unfortmated values
        data = remove_unformated_character(data)
        #Remove unicode characters
        for i in columns_that_need_unidecode:
            data[i] = remove_unicode(data[i])
        #Check dictionary
        data, df_dict = check_dictionary(df_dict, file_name, data, 'City', 'City', sector, engine, sp_object)
        data, df_dict = check_dictionary(df_dict, file_name, data, 'Indicator', 'Indicator', sector, engine, sp_object)
        print(df_dict)
        if len(df_dict) == 0:
            print(colored('Validate succesfully','green'))
            data = Generate_Additional_Columns(data,df_summ_file, Hub ,engine ,file_url)
            df_temp_flat_macro = pd.concat([df_temp_flat_macro, data], axis=0)
            df_flat_macro = tracking_flat_file(df_temp_flat_macro, sector)
            insert_to_fresh_Macro(file_url, data, cnt_str)
            
        else:
            pass


def main():
    cnt_str = ConnectAzureSQLServer()
    engine = create_engine(cnt_str)
    # set to_email
    to_email = ['hcmcbi-intern04@savills.com.vn']
    selected_provinces = [] 
    while(True):
        Sector= 'Macro Economic'
        url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/{Sector}/Flat file'
        Hub= ConnectSharePoint(url_hub)
        sp_object = url_hub.split('/')[2].replace('-','')
        list_folder = Hub.get_content_url(url_hub, return_list_folder=True)
        if (Sector == 'Macro Economic' or Sector == 'macro economic'):
            list_url, df_summ_file = getFile(list_folder, selected_provinces, url_hub)
            selected_provinces.clear()
            importMacroEconomic(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)


if __name__ == "__main__":
    main()