from Connection import *

class MacroScript:
    def findFolderWithProvince(list_folder, selected_provinces  ,Year, Quarter):
        # Tìm các folder chứa từ khóa
        selected_province_folders = [folder for folder in list_folder if any(re.search(keyword, folder, re.IGNORECASE) for keyword in selected_provinces)]
        selected_year_quarter = []
        for folder in selected_province_folders:
            folder_sub2 = folder + '/' + Year + '/' + Quarter
            selected_year_quarter.append(folder_sub2)
        # In danh sách các folder đã tìm thấy
        return selected_year_quarter

    def findFolderWithYearQuar(list_folder,selected_provinces , url_hub, year, quarter):
        df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
        for i in MacroScript.findFolderWithProvince(list_folder, selected_provinces, year, quarter):
            if i.split('/')[7] in selected_provinces and i.split('/')[8] in year and i.split('/')[9] in quarter:
                df_summ_file = pd.concat([df_summ_file, ConnectSharePoint(url_hub).get_content_url(i)])
        list_url = df_summ_file['ServerRelativeUrl'].to_list()
        #History file
        df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
        df_summ_file = df_summ_file.reset_index(drop=True)
        return list_url , df_summ_file
    
    def findFolderMacroCommentary(list_folder, list_sub, url_hub, year, quarter):
        df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
        for i in list_sub:
            if i.split('/')[-2] in year and i.split('/')[-1] in quarter:
                df_summ_file = pd.concat([df_summ_file, ConnectSharePoint(url_hub).get_content_url(i)])
        list_file = df_summ_file['ServerRelativeUrl'].to_list()
        #History file
        df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
        df_summ_file = df_summ_file.reset_index(drop=True)
        return df_summ_file

    
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
        22

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
        MacroScript.findFolderWithProvince(list_folder, selected_provinces,Year,Quarter)
        list_url , df_summ_file = MacroScript.findFolderWithYearQuar(list_folder, selected_provinces, url_hub, Year, Quarter)
        list_url = getData.getListImportFile(list_url)
        return list_url, df_summ_file
class importData:
    def importIP(list_folder, url_hub, list_url, to_email, columns_that_need_unidecode,cnt_str, engine, sp_object,df_summ_file, Hub):
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
                    data = Generate_Additional_Columns(data,list_url,df_summ_file, Hub ,engine,file_url)
                    processed_data, flag_key = check_project_key(file_url, data, sector, engine)
                    df_temp_flat_ip = pd.concat([df_temp_flat_ip, data], axis=0)
                    df_flat_ip = tracking_flat_file(df_temp_flat_ip, file_url)
                    if len(processed_data) != 0:
                        df_new_key_ip = check_new_key(df_new_key = df_new_key_ip, processed_data = processed_data, sector = sector)
                    #Get key and generate new key (if needed)
                    data = get_project_key(flag_key, processed_data, data, sector, engine)
                    # insert_to_fresh(file_url, data, cnt_str)
                    
                    #Test insert_to_fresh
                    try:
                        result = insert_to_fresh(file_url, data, cnt_str)
                        print(colored("insert_to_fresh SUCESSFUL!",'green'))
                    except Exception as e:
                        print(colored("insert_to_fresh FAILED:",str(e),'red'))
                    
                else:
                    print(df_dict)
                    print(colored('Validate failed','red'))
                    pass
           

def main():
    
    columns_that_need_unidecode=['Project_Name', 'Sub_Project_Name', 'Developer_Name'
                             , 'City', 'District', 'Target_Industry']
    Sector= input("Nhập Sector bạn muốn lấy data: ")
    # set to_email
    to_email = ['nthieu@savills.com.vn', 'hcmcbi-intern04@savills.com .vn']
    url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/{Sector}/Flat file'
    sp_object = url_hub.split('/')[2].replace('-','')
    Hub = ConnectSharePoint(url_hub)
    list_folder = Hub.get_content_url(url_hub, return_list_folder=True) 
    selected_provinces = [] 
    cnt_str, engine = ConnectAzureSQLServer()
    if (Sector == 'Macro Economic'):

        getData.getFile(list_folder, selected_provinces, url_hub)
    if (Sector == 'Macro commentary'):
        quarter_year = input("Nhập chuỗi năm và quý (ví dụ: 2023Q1): ")
        Year, Quarter = quarter_year[:4], quarter_year[4:]
        MacroScript.findFolderMacroCommentary(list_folder, getData.getQuarter(list_folder, url_hub), url_hub, Year, Quarter)
    if (Sector == 'IP'):
        list_url, df_summ_file  = getData.getFile(list_folder, selected_provinces, url_hub)
        importData.importIP(list_folder, url_hub, list_url, to_email, columns_that_need_unidecode,cnt_str, engine, sp_object,df_summ_file, Hub)

if __name__ == "__main__":
    main()
