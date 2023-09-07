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
def findFolderWithSector(list_folder, selected_sector):
    # Tìm các folder chứa từ khóa
    selected_sector_folders = [folder for folder in list_folder if any(re.search(keyword, folder, re.IGNORECASE) for keyword in selected_sector)]
    return selected_sector_folders

def findFolder(list_folder, selected_sector , url_hub):
    df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
    for i in findFolderWithSector(list_folder, selected_sector):
        if i.split('/')[7] in selected_sector:
            df_summ_file = pd.concat([df_summ_file, ConnectSharePoint(url_hub).get_content_url(i)])
    list_url = df_summ_file['ServerRelativeUrl'].to_list()
    #History file
    df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
    df_summ_file = df_summ_file.reset_index(drop=True)
    return list_url , df_summ_file
    

def getFileMainSector(list_folder, url_hub):
    # Input provinces
    selected_sector = input("Nhập sector: ")
    list_url , df_summ_file = findFolder(list_folder, selected_sector, url_hub)
    list_url = getListImportFile(list_url)
    return list_url, df_summ_file

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
        data, file_name, sector = get_data(url_hub, file_url)
        #-------------------------------------------------------
        #data = check_date_key(file_url, data) #Check format date_key in flat file   
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
            # Check dictionary
            lst_dict = ['City', 'District', 'Status', 'Grade', 'Type']
            lst_cls = ['Project_City_Name', 'Project_District_Name', 'Project_Status', 'Grade', 'Sub_Project_Type']
            # lst_dict = ['City', 'District', 'Status']
            # lst_cls = ['Project_City_Name', 'Project_District_Name', 'Project_Status']
            #print('Before: \n',before_check)
            for i, j in zip(lst_cls, lst_dict):
                data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
            # after_check = data['Sub_Project_Type'].value_counts()
            # print('After: \n',after_check)
            print(df_dict)
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
                        df_new_key_retail= pd.DataFrame()
                        df_new_key_retail = check_new_key(df_new_key = df_new_key_retail, processed_data = processed_data, sector = sector)
                elif sector == 'OFFICE':
                    df_temp_flat_office = pd.DataFrame()
                    df_temp_flat_office = pd.concat([df_temp_flat_office, data], axis=0)
                    df_flat_office = tracking_flat_file(df_temp_flat_office, file_url)
                    if len(processed_data) != 0:
                        df_new_key_office= pd.DataFrame()
                        df_new_key_office = check_new_key(df_new_key = df_new_key_office, processed_data = processed_data, sector = sector)
                elif sector == 'HOTEL':
                    df_temp_flat_hotel = pd.DataFrame()
                    df_temp_flat_hotel = pd.concat([df_temp_flat_hotel, data], axis=0)
                    df_flat_hotel = tracking_flat_file(df_temp_flat_hotel, file_url)
                    if len(processed_data) != 0:
                        df_new_key_hotel= pd.DataFrame()
                        df_new_key_hotel = check_new_key(df_new_key = df_new_key_hotel, processed_data = processed_data, sector = sector)
                elif sector == 'SA' or sector=='SERVICED_APARTMENT':
                    df_temp_flat_sa = pd.DataFrame()
                    df_temp_flat_sa = pd.concat([df_temp_flat_sa, data], axis=0)
                    df_flat_sa = tracking_flat_file(df_temp_flat_sa, file_url)
                    if len(processed_data) != 0:
                        df_new_key_sa= pd.DataFrame()
                        df_new_key_sa = check_new_key(df_new_key = df_new_key_sa, processed_data = processed_data, sector = sector)
                elif sector == 'APT' or sector=='APARTMENT':
                    df_temp_flat_apt = pd.DataFrame()
                    df_temp_flat_apt = pd.concat([df_temp_flat_apt, data], axis=0)
                    df_flat_apt = tracking_flat_file(df_temp_flat_apt, file_url)
                    if len(processed_data) != 0:
                        df_new_key_apt= pd.DataFrame()
                        df_new_key_apt = check_new_key(df_new_key = df_new_key_apt, processed_data = processed_data, sector = sector)
                
                elif sector == 'VLTH':
                    df_temp_flat_vlth = pd.DataFrame()
                    df_temp_flat_vlth = pd.concat([df_temp_flat_vlth, data], axis=0)
                    df_flat_vlth = tracking_flat_file(df_temp_flat_vlth, file_url)
                    if len(processed_data) != 0:
                        df_new_key_vlth= pd.DataFrame()
                        df_new_key_vlth = check_new_key(df_new_key = df_new_key_vlth, processed_data = processed_data, sector = sector) 
                else:
                    pass
                #Get key and generate new key (if needed)
                data = get_project_key(flag_key, processed_data, data, sector, engine)
                try:
                    #result = insert_to_fresh(file_url, data, cnt_str)
                    print(colored("insert_to_fresh SUCESSFUL!",'green'))
                except Exception as e:
                    print(colored("insert_to_fresh FAILED:",str(e),'red'))
            else:
                pass

def main():
    cnt_str = ConnectAzureSQLServer()
    engine = create_engine(cnt_str)

    # set to_email
    to_email = ['hcmcbi-intern04@savills.com.vn', 'nthieu@savills.com.vn']
    while(True):
        url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/Main Sector/Flat file'
        Hub= ConnectSharePoint(url_hub)
        sp_object = url_hub.split('/')[2].replace('-','')
        list_folder = Hub.get_content_url(url_hub, return_list_folder=True)
        list_url, df_summ_file = getFileMainSector(list_folder, url_hub) 
        importMainSector(list_folder, url_hub, list_url ,cnt_str, sp_object,df_summ_file, Hub)


if __name__ == "__main__":
    main()