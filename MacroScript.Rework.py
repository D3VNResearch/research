from Connection import *

class MacroScript:
    def findFolderMacroEconomic(list_folder, selected_provinces, url_hub ,Year, Quarter):
        # Tìm các folder chứa từ khóa
        selected_province_folders = [folder for folder in list_folder if any(re.search(keyword, folder, re.IGNORECASE) for keyword in selected_provinces)]
        selected_year_quarter = []
        for folder in selected_province_folders:
            folder_sub2 = folder + '/' + Year + '/' + Quarter
            selected_year_quarter.append(folder_sub2)
        # In danh sách các folder đã tìm thấy
        print("Folder các tỉnh cần import:")
        for folder in selected_province_folders:
            print(colored(folder,'yellow'))

        #Lấy file trong Năm và Quý
        df_summ_file = pd.DataFrame({'Name':[],'ServerRelativeUrl':[], 'TimeLastModified':[], 'ModTime':[], 'Modified_by_ID':[]})
        for i in selected_year_quarter:
            if i.split('/')[7] in selected_provinces and i.split('/')[8] in Year and i.split('/')[9] in Quarter:
                df_summ_file = pd.concat([df_summ_file, ConnectSharePoint(url_hub).get_content_url(i)])
        list_url = df_summ_file['ServerRelativeUrl'].to_list()
        #History file
        df_query=pd.DataFrame(df_summ_file).reset_index(drop=True)
        df_summ_file = df_summ_file.reset_index(drop=True)
        return df_summ_file
    

    
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


def main():
    Sector= input("Nhập Sector bạn muốn lấy data: ")
    # set to_email
    to_email = ['nthieu@savills.com.vn', 'hcmcbi-intern04@savills.com .vn']
    url_hub=f'/sites/BIHub/Shared Documents/Advisory Data/{Sector}/Flat file'
    list_folder = ConnectSharePoint(url_hub).get_content_url(url_hub, return_list_folder=True) 
    selected_provinces = [] 
    ConnectAzureSQLServer()
    if (Sector == 'Macro Economic'):
        # Input provinces
        input_string = input("Nhập tên các tỉnh bạn muốn chọn, cách nhau bằng dấu phẩy (nhập -1 để thoát): ")
        # Input Năm và Quý
        quarter_year = input("Nhập chuỗi năm và quý (ví dụ: 2023Q1): ")
        Year, Quarter = quarter_year[:4], quarter_year[4:]

        # In danh sách tỉnh đã chọn, năm và quý
        print("Các tỉnh đã chọn:")
        for province in MacroScript.GetProvince(input_string, list_folder ,selected_provinces):
            print(colored(province,'yellow'))
        print(colored("Year:{}, Quarter:{}".format(Year,Quarter),'yellow'))
        MacroScript.findFolderMacroEconomic(list_folder, selected_provinces , url_hub,Year, Quarter)
    if (Sector == 'Macro commentary'):
        inputYear= input('Nhập năm: ')
        inputQuarter =input('Nhập quý: ')
        MacroScript.findFolderMacroCommentary(list_folder, MacroScript.getQuarter(list_folder, url_hub), url_hub, inputYear, inputQuarter)
if __name__ == "__main__":
    main()
