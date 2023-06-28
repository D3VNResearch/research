from Connection import *

class findFolder:
    def findFolderWithProvince(list_folder, selected_provinces  ,Year, Quarter):
        # Tìm các folder chứa từ khóa
        selected_province_folders = [folder for folder in list_folder if any(re.search(keyword, folder, re.IGNORECASE) for keyword in selected_provinces)]
        selected_year_quarter = []
        for folder in selected_province_folders:
            folder_sub2 = folder + '/' + Year + '/' + Quarter
            selected_year_quarter.append(folder_sub2)
        # In danh sách các folder đã tìm thấy
        return selected_year_quarter

    def findFolder(list_folder,selected_provinces , url_hub, year, quarter):
        df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
        for i in findFolder.findFolderWithProvince(list_folder, selected_provinces, year, quarter):
            if i.split('/')[7] in selected_provinces and i.split('/')[8] in year and i.split('/')[9] in quarter:
                df_summ_file = pd.concat([df_summ_file, ConnectSharePoint(url_hub).get_content_url(i)])
        list_url = df_summ_file['ServerRelativeUrl'].to_list()
        #History file
        df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
        df_summ_file = df_summ_file.reset_index(drop=True)
        return list_url , df_summ_file
    
    def findFolderYearToQuar(list_folder, list_sub, Hub, year, quarter): ### Apply for Macro Commentary, FDI
        df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
        for i in list_sub:
            if i.split('/')[-2] in year and i.split('/')[-1] in quarter:
                df_summ_file = pd.concat([df_summ_file, Hub.get_content_url(i)])
        list_url = df_summ_file['ServerRelativeUrl'].to_list()
        #History file
        df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
        df_summ_file = df_summ_file.reset_index(drop=True)
        return list_url, df_summ_file

    
class getData:
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
    def getYear():
        return
    def getQuarter(list_folder,url_hub):
        list_folder_sub1=[]
        for i in list_folder:
            ls=ConnectSharePoint(url_hub).get_content_url(i,return_list_folder=True)
            list_folder_sub1=list_folder_sub1+ls
        return list_folder_sub1
    def getListImportFile(list_url):
        url = []
        for i in list_url:
            sector = i.split('/')[-1].split('_')[0].upper()
            if sector not in ('x'):
                url.append(i)
            else:
                pass
        return url
        
    def getFileMainSector(list_folder, selected_provinces, url_hub):
        url = []
        # Input provinces
        input_string = input("Nhập tên các tỉnh bạn muốn chọn, cách nhau bằng dấu phẩy (nhập -1 để thoát): ")
        # Input Năm và Quý
        quarter_year = input("Nhập chuỗi năm và quý (ví dụ: 2023Q1): ")
        Year, Quarter = quarter_year[:4], quarter_year[4:]

        # In danh sách tỉnh đã chọn, năm và quý
        print("Các tỉnh đã chọn:")
        for province in getData.GetProvince(input_string, list_folder ,selected_provinces):
            print(colored(province,'yellow'))
        print(colored("Year:{}, Quarter:{}".format(Year,Quarter),'yellow'))
        findFolder.findFolderWithProvince(list_folder, selected_provinces,Year,Quarter)
        list_url , df_summ_file = findFolder.findFolder(list_folder, selected_provinces, url_hub, Year, Quarter)
        sector = input('Nhập sector ()')
        for i in list_url:
            sector = i.split('/')[-1].split('_')[0].upper()
            
            if sector=='APARTMENT':
                sector='APT'
            else:
                pass
            
            if sector in ('VLTH'):#Chọn sector cần import: RETAIL, OFFICE, SA, HOTEL, APT, VLTH
                url.append(i)
            else:
                pass
        return url, df_summ_file
        
    def getFile(list_folder, selected_provinces, url_hub):
        # Input provinces
        input_string = input("Nhập tên các tỉnh bạn muốn chọn, cách nhau bằng dấu phẩy (nhập -1 để thoát): ")
        # Input Năm và Quý
        quarter_year = input("Nhập chuỗi năm và quý (ví dụ: 2023Q1): ")
        Year, Quarter = quarter_year[:4], quarter_year[4:]

        # In danh sách tỉnh đã chọn, năm và quý
        print("Các tỉnh đã chọn:")
        for province in getData.GetProvince(input_string, list_folder ,selected_provinces):
            print(colored(province,'yellow'))
        print(colored("Year:{}, Quarter:{}".format(Year,Quarter),'yellow'))
        findFolder.findFolderWithProvince(list_folder, selected_provinces,Year,Quarter)
        list_url , df_summ_file = findFolder.findFolder(list_folder, selected_provinces, url_hub, Year, Quarter)
        list_url = getData.getListImportFile(list_url)
        return list_url, df_summ_file
class importData:
    def importIP(list_folder, url_hub, list_url, to_email,cnt_str, sp_object,df_summ_file, Hub):
        columns_that_need_unidecode=['Project_Name', 'Sub_Project_Name', 'Developer_Name'
                             , 'City', 'District', 'Target_Industry']
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
            data, file_name, sector = get_data(url_hub, file_url)
            #-------------------------------------------------------
            data = check_date_key(file_url, data)#Check format date_key in flat file   
            data['Project_Name']= np.where(data['Project_Name'].isnull(), data['Sub_Project_Name'], data['Project_Name'])#Fill up project_name if its null
            data['Project_Sub_Type']= np.where(data['Project_Sub_Type'].isnull(), data['Project_Type'], data['Project_Sub_Type'])#Fill up Sub_Type if its null
            
            #Check duplicate sub_name
            print('Start Check duplicate sub_name...')
            data, df_dup = check_duplicate(data, 'Sub_Project_Name')
            if len(df_dup) != 0:
                print(colored('Check duplicate sub_name', 'yellow'))
                df_noti_html = convert_df_to_html(type_html = 1, df = df_dup, type_sector = 2, cnxn = engine)
                run_email(type_sector = 'IP', email_type = 1, user_email = to_email, df_noti_html = df_noti_html)
            else:       
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
                    #Get key and generate new key (if needed)
                    data = get_project_key(flag_key, processed_data, data, sector, engine)
                    #insert_to_fresh(file_url, data, cnt_str)
                    
                    #Test insert_to_fresh
                    try:
                        result = insert_to_fresh(file_url, data, cnt_str)
                        print(colored("insert_to_fresh SUCESSFUL!",'green'))
                    except Exception as e:
                        print('er')
                        print(colored("insert_to_fresh FAILED:",str(e),'red'))
                    
                else:
                    print(df_dict)
                    print(colored('Validate failed','red'))
                    pass
    





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
            data, file_name, sector = get_data(url_hub, file_url)
            #-------------------------------------------------------
            data = check_date_key(file_url, data)#Check format date_key in flat file
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
            if len(df_dict) == 0:
                print(colored('Validate succesfully','green'))
                data = Generate_Additional_Columns(data,df_summ_file, Hub ,engine ,file_url)
                df_temp_flat_macro = pd.concat([df_temp_flat_macro, data], axis=0)
                df_flat_macro = tracking_flat_file(df_temp_flat_macro, sector)
                insert_to_fresh(file_url, data, cnt_str)
            else:
                pass


    def importMacroCommentary(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub):
        '''Prepare ingredients'''
        columns_that_need_unidecode=['City', 'Commentary_Image'
                                    ]
        #Create empty df for checking dictionary
        df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
        engine = create_engine(cnt_str)
        #-------------------------------------------------------
        '''Get data''' 
        for file_url in tqdm(list_url):
            data, file_name, sector = get_data(url_hub, file_url)
            #-------------------------------------------------------
            data = check_date_key(file_url, data)#Check format date_key in flat file
            #-------------------------------------------------------
            '''Validation step'''
            #Remove unfortmated values
            data = remove_unformated_character(data)
            #Remove unicode characters
            for i in columns_that_need_unidecode:
                data[i] = remove_unicode(data[i])
            #Check dictionary
            lst_dict = ['City']
            lst_cls = ['City']
            for i, j in zip(lst_cls, lst_dict):
                data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
            if len(df_dict) == 0:
                print(colored('Validate succesfully','green'))
                data = Generate_Additional_Columns(data,df_summ_file, Hub,engine,file_url)
                data.to_sql('Macro_Commentary', engine, index=False, if_exists='append', schema='Fresh')
            else:
                pass

    def importFDI(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub):
        '''Prepare ingredients'''
        columns_that_need_unidecode=['Project_Name', 'Business_Name', 'Investor_Name', 'Investor_Nationality'
                                    , 'Industry_Lv1', 'Investment_Form', 'City', 'District'
                                    ]
        #Create empty df for checking dictionary
        df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
        df_temp_flat_fdi = pd.DataFrame()
        engine = create_engine(cnt_str)
        #-------------------------------------------------------
        '''Get data''' 
        for file_url in tqdm(list_url):
            data, file_name, sector = get_data(url_hub, file_url)
            #-------------------------------------------------------
            data = check_date_key(file_url, data)#Check format date_key in flat file
            #-------------------------------------------------------
            '''Validation step'''
            #Remove unfortmated values
            #data = remove_unformated_character(type_sector = 2, sector = sector, file_name = file_name, data = data)
            data = remove_unformated_character(data)
            #Remove unicode characters
            for i in columns_that_need_unidecode:
                data[i] = remove_unicode(data[i])
            #Check dictionary
            lst_dict = ['City', 'District', 'Project_Name', 'Investor_Nationality', 'Industry_Lv1', 'Investment_Form']
            lst_cls = ['City', 'District', 'Project_Name', 'Investor_Nationality', 'Industry_Lv1', 'Investment_Form']
            for i, j in zip(lst_cls, lst_dict):
                data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
            if len(df_dict) == 0:
                print(colored('Validate succesfully','green'))
                data = Generate_Additional_Columns(data,df_summ_file,Hub,engine,file_url)
                df_temp_flat_fdi = pd.concat([df_temp_flat_fdi, data], axis=0)
                df_flat_fdi = tracking_flat_file(df_temp_flat_fdi, sector)
                insert_to_fresh(file_url, df_flat_fdi, cnt_str)
            else:
                pass


    def importInfra(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub):
        '''Prepare ingredients'''
        columns_that_need_unidecode=['Infratructure_Project', 'District', 'City', 'Region'
                                    , 'Infrastructure_Level_1', 'Infrastructure_Level_2', 'Source_Link'
                                    ]
        #Create empty df for checking dictionary
        df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
        df_temp_flat_infra = pd.DataFrame()
        df_flat_infra = pd.DataFrame()
        engine = create_engine(cnt_str)
        #-------------------------------------------------------
        '''Get data''' 
        for file_url in tqdm(list_url):
            data, file_name, sector = get_data(url_hub, file_url)
            #-------------------------------------------------------
            data = check_date_key(file_url, data)#Check format date_key in flat file
            #-------------------------------------------------------
            '''Validation step'''
            #Remove unfortmated values
            data = remove_unformated_character(data)
            #Remove unicode characters
            for i in columns_that_need_unidecode:
                data[i] = remove_unicode(data[i])
            #Check dictionary
            lst_dict = ['City', 'District', 'Infrastructure_Level_1', 'Infrastructure_Level_2']
            lst_cls = ['City', 'District', 'Infrastructure_Level_1', 'Infrastructure_Level_2']
            for i, j in zip(lst_cls, lst_dict):
                data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
            if len(df_dict) == 0:
                print(colored('Validate succesfully','green'))
                data = Generate_Additional_Columns(data,df_summ_file,Hub,engine,file_url)
                df_temp_flat_infra = pd.concat([df_temp_flat_infra, data], axis=0)
                df_flat_infra = tracking_flat_file(df_temp_flat_infra, sector)
                #Test insert_to_fresh
                try:
                    result = insert_to_fresh(file_url, data, cnt_str)
                    print(colored("insert_to_fresh SUCESSFUL!",'green'))
                except Exception as e:
                    print(colored("insert_to_fresh FAILED:",str(e),'red'))
            else:
                pass

    def importProductMix(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub):
        '''Prepare ingredients''' 
        columns_that_need_unidecode=['Project_Name','Sub_Project_Name', 'Mix_Name'
                                    ,'Sub_Project_Type', 'Project_City_Name', 'Project_District_Name']
        #Create empty df for checking dictionary
        df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
        engine = create_engine(cnt_str)
        #Create multi empty df for tracking autdit step
        name_sector = ['APT', 'VLTH']
        name_sector = [x.lower() for x in name_sector]
        for i in name_sector:
            globals()['df_temp_flat_{}'.format(i)] = pd.DataFrame([])
            globals()['df_flat_{}'.format(i)] = pd.DataFrame([])
        #-------------------------------------------------------
        '''Get data''' 
        for file_url in tqdm(list_url):  
            data, file_name, sector = get_data(url_hub, file_url)
            #-------------------------------------------------------
            data = check_date_key(file_url, data)#Check format date_key in flat file   
            '''Check duplicate mix_name'''
            #data, df_dup = check_duplicate(data, 'Mix_Name')   
            #-------------------------------------------------------
            '''Validation step'''
            #Remove unfortmated values
            data = remove_unformated_character(data)
            #Remove unicode characters
            for i in columns_that_need_unidecode:
                data[i] = remove_unicode(data[i])
            #Check dictionary
            lst_dict = ['City', 'District', 'Type', 'Grade']
            lst_cls = ['Project_City_Name', 'Project_District_Name', 'Sub_Project_Type', 'Grade']
            for i, j in zip(lst_cls, lst_dict):
                data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
            if len(df_dict) == 0:
                print(colored('Validate succesfully','green'))
                #-------------------------------------------------------
                '''Import data process'''
                #Check project key
                data = Generate_Additional_Columns(data,df_summ_file,Hub,engine,file_url)
                processed_data, flag_key = check_project_key(file_url, data, sector, engine)
                if flag_key == 0:
                    #Create tracking audit
                    if sector == 'APT' or sector=='APARTMENT':
                        df_temp_flat_apt = pd.DataFrame()
                        df_temp_flat_apt = pd.concat([df_temp_flat_apt, data], axis=0)
                        df_flat_apt = tracking_flat_file(df_temp_flat_apt, file_url)
                    elif sector == 'VLTH':
                        df_temp_flat_vlth = pd.DataFrame()
                        df_temp_flat_vlth = pd.concat([df_temp_flat_vlth, data], axis=0)
                        df_flat_vlth = tracking_flat_file(df_temp_flat_vlth, file_url)
                    else:
                        pass
                    #Get key 
                    #data = get_project_key(flag_key, processed_data, data, sector, engine)
                    #insert_to_fresh(file_url, data, engine)
                else:
                    print(colored(f'Cant get key in {file_name}', 'yellow'))
            else:
                pass

    def importMainSector(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub):
        '''Prepare ingredients''' 
        columns_that_need_unidecode = ['Project_Name','Sub_Project_Name','Developer','Constructor'
                                    , 'Operator','Project_District_Name','Project_City_Name', 'Construction_Status'
                                    ]
        engine = create_engine(cnt_str)
        #Create empty df for checking dictionary
        df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
        #Create multi empty df for tracking autdit step
        name_sector = ['RETAIL', 'SA', 'APT', 'VLTH', 'OFFICE', 'HOTEL']
        name_sector = [x.lower() for x in name_sector]
        for i in name_sector:
            globals()['df_temp_flat_{}'.format(i)] = pd.DataFrame([])
            globals()['df_flat_{}'.format(i)] = pd.DataFrame([])
            globals()['df_new_key_{}'.format(i)] = pd.DataFrame([])  
        #-------------------------------------------------------
        '''Get data''' 
        for file_url in tqdm(list_url):  
            data, file_name, sector = get_data(url_hub, file_url)
            #-------------------------------------------------------
            data = check_date_key(file_url, data)#Check format date_key in flat file   
            data['Project_Name']= np.where(data['Project_Name'].isnull(), data['Sub_Project_Name'], data['Project_Name']) #Fill up project_name if its null
            #Check duplicate sub_name
            data, df_dup = check_duplicate(data, 'Sub_Project_Name')
            if len(df_dup) != 0:
                print(colored('Check dupplicate sub_name', 'yellow'))
                df_noti_html = convert_df_to_html(type_html = 1, df = df_dup, type_sector = 1, cnxn = engine)
                to_email = ['nthieu@savills.com.vn', 'hcmcbi-intern04@savills.com.vn']
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
                #Check dictionary
                lst_dict = ['City', 'District', 'Status', 'Type', 'Grade']
                lst_cls = ['Project_City_Name', 'Project_District_Name', 'Project_Status', 'Sub_Project_Type', 'Grade']
                for i, j in zip(lst_cls, lst_dict):
                    data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
                if len(df_dict) == 0:
                    print(colored('Validate succesfully','green'))
                    #-------------------------------------------------------
                    '''Import data process'''
                    #Check project key
                    data = Generate_Additional_Columns(data,df_summ_file,Hub,engine,file_url)
                    processed_data, flag_key = check_project_key(file_url, data, sector, engine)
                    #Create tracking audit
                    if sector == 'RETAIL':
                        df_temp_flat_retail = pd.DataFrame()
                        df_temp_flat_retail = pd.concat([df_temp_flat_retail, data], axis=0)
                        df_flat_retail=tracking_flat_file(df_temp_flat_retail, file_url)
                        if len(processed_data) != 0:
                            df_new_key_retail = check_new_key(df_new_key = df_new_key_retail, processed_data = processed_data, sector = sector)
                    elif sector == 'OFFICE':
                        df_temp_flat_office = pd.DataFrame()
                        df_temp_flat_office = pd.concat([df_temp_flat_office, data], axis=0)
                        df_flat_office = tracking_flat_file(df_temp_flat_office, file_url)
                        if len(processed_data) != 0:
                            df_new_key_office = check_new_key(df_new_key = df_new_key_office, processed_data = processed_data, sector = sector)
                    elif sector == 'HOTEL':
                        df_temp_flat_hotel = pd.DataFrame()
                        df_temp_flat_hotel = pd.concat([df_temp_flat_hotel, data], axis=0)
                        df_flat_hotel = tracking_flat_file(df_temp_flat_hotel, file_url)
                        if len(processed_data) != 0:
                            df_new_key_hotel = check_new_key(df_new_key = df_new_key_hotel, processed_data = processed_data, sector = sector)
                    elif sector == 'SA' or sector=='SERVICED_APARTMENT':
                        df_temp_flat_sa = pd.DataFrame()
                        df_temp_flat_sa = pd.concat([df_temp_flat_sa, data], axis=0)
                        df_flat_sa = tracking_flat_file(df_temp_flat_sa, file_url)
                        if len(processed_data) != 0:
                            df_new_key_sa = check_new_key(df_new_key = df_new_key_sa, processed_data = processed_data, sector = sector)
                    elif sector == 'APT' or sector=='APARTMENT':
                        df_temp_flat_apt = pd.DataFrame()
                        df_temp_flat_apt = pd.concat([df_temp_flat_apt, data], axis=0)
                        df_flat_apt = tracking_flat_file(df_temp_flat_apt, file_url)
                        if len(processed_data) != 0:
                            df_new_key_apt = check_new_key(df_new_key = df_new_key_apt, processed_data = processed_data, sector = sector)
                    
                    elif sector == 'VLTH':
                        df_temp_flat_vlth = pd.DataFrame()
                        df_temp_flat_vlth = pd.concat([df_temp_flat_vlth, data], axis=0)
                        df_flat_vlth = tracking_flat_file(df_temp_flat_vlth, file_url)
                        if len(processed_data) != 0:
                            df_new_key_vlth = check_new_key(df_new_key = df_new_key_vlth, processed_data = processed_data, sector = sector) 
                    else:
                        pass
                    #Get key and generate new key (if needed)
                    data = get_project_key(flag_key, processed_data, data, sector, engine)
                    insert_to_fresh(file_url, data, cnt_str)
                else:
                    pass



def main():
    cnt_str = ConnectAzureSQLServer()
    # set to_email
    to_email = ['nthieu@savills.com.vn']
    selected_provinces = [] 
    while(True):
        Sector= input("Nhập Sector bạn muốn lấy data: ")
        url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/{Sector}/Flat file'
        Hub= ConnectSharePoint(url_hub)
        sp_object = url_hub.split('/')[2].replace('-','')
        list_folder = Hub.get_content_url(url_hub, return_list_folder=True)
        if (Sector == 'Macro Economic' or Sector == 'macro economic'):
            list_url, df_summ_file = getData.getFile(list_folder, selected_provinces, url_hub)
            selected_provinces.clear()
            importData.importMacroEconomic(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)
        if (Sector == 'Infra'):
            list_url, df_summ_file = getData.getFile(list_folder, selected_provinces, url_hub)
            selected_provinces.clear()
            importData.importInfra(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)
        if (Sector == 'Macro commentary'):
            quarter_year = input("Nhập chuỗi năm và quý (ví dụ: 2023Q1): ")
            Year, Quarter = quarter_year[:4], quarter_year[4:]
            list_url, df_summ_file = findFolder.findFolderYearToQuar(list_folder, getData.getQuarter(list_folder, url_hub), Hub, Year, Quarter)
            selected_provinces.clear()
            importData.importMacroCommentary(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)
        # if (Sector == 'FDI'):   ####Cai nay loi~ :((((
        #     quarter_year = input("Nhập chuỗi năm và quý (ví dụ: 2023Q1): ")
        #     Year, Quarter = quarter_year[:4], quarter_year[4:]
        #     list_url, df_summ_file = findFolder.findFolderYearToQuar(list_folder, getData.getQuarter(list_folder, url_hub), Hub, Year, Quarter)
        #     selected_provinces.clear()
        #     importData.importFDI(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)
        if (Sector == 'IP'):
            list_url, df_summ_file = getData.getFile(list_folder, selected_provinces, url_hub) 
            print(list_url)
            selected_provinces.clear()
            importData.importIP(list_folder, url_hub, list_url, to_email,cnt_str, sp_object,df_summ_file, Hub)
        if (Sector == 'Main Sector'):
            list_url, df_summ_file = getData.getFile(list_folder, selected_provinces, url_hub) 
            selected_provinces.clear()
            importData.importMainSector(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)
        if(Sector == 'Product Mix'):
            list_url, df_summ_file = getData.getFile(list_folder, selected_provinces, url_hub) 
            selected_provinces.clear()
            importData.importProductMix(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)
if __name__ == "__main__":
    main()




