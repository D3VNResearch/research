import pandas as pd
import numpy as np
from tqdm import tqdm
from termcolor import colored

class IPData:
    def __init__(self, urls, columns_that_need_unidecode, cnt_str):
        self.urls = urls
        self.columns_that_need_unidecode = columns_that_need_unidecode
        self.df_dict = pd.DataFrame(columns=['File_Name', 'Missing_Values', 'Flag'])
        self.df_temp_flat_ip = pd.DataFrame([])
        self.df_flat_ip = pd.DataFrame([])
        self.df_new_key_ip = pd.DataFrame([])
        self.cnt_str = cnt_str

    def get_data(self, file_url):
        # Existing function call
        data, file_name, sector = get_data(relative_url, file_url)
        return data, file_name, sector

    def check_duplicate(self, data):
        # Existing function call
        data, df_dup = check_duplicate(data, 'Sub_Project_Name')
        return data, df_dup

    def check_dictionary(self, df_dict, file_name, data, i, j, sector):
        # Existing function call
        data, df_dict = check_dictionary(df_dict, file_name, data, i, j, sector, engine, sp_object)
        return data, df_dict

    def process_data(self):
        print('Start Get Data...')
        for file_url in tqdm(self.urls):
            data, file_name, sector = self.get_data(file_url)

            data = check_date_key(file_url, data)
            data['Project_Name'] = np.where(data['Project_Name'].isnull(), data['Sub_Project_Name'], data['Project_Name'])
            data['Project_Sub_Type'] = np.where(data['Project_Sub_Type'].isnull(), data['Project_Type'], data['Project_Sub_Type'])

            print('Start Check duplicate sub_name...')
            data, df_dup = self.check_duplicate(data)

            if len(df_dup) != 0:
                print(colored('Check duplicate sub_name', 'yellow'))
                df_noti_html = convert_df_to_html(type_html=1, df=df_dup, type_sector=2, cnxn=engine)
                run_email(type_sector='IP', email_type=1, user_email=to_email, df_noti_html=df_noti_html)
            else:
                print('Start validation step...')
                data = remove_unformated_character(data)

                for i in self.columns_that_need_unidecode:
                    data[i] = remove_unicode(data[i])

                print('Start Check dictionary...')
                lst_dict = ['City', 'District', 'Status', 'Sub_Type', 'Type']
                lst_cls = ['City', 'District', 'Status', 'Project_Sub_Type', 'Project_Type']

                for i, j in zip(lst_cls, lst_dict):
                    data, self.df_dict = self.check_dictionary(self.df_dict, file_name, data, i, j, sector)

                print(f'End Check dictionary with df_dict = {len(self.df_dict)} ...')

                if len(self.df_dict) == 0:
                    print(colored('Validate successfully', 'green'))

                    data = Generate_Additional_Columns(data, url, df_summ_file, BIHub, engine, file_url)
                    processed_data, flag_key = check_project_key(file_url, data, sector, engine)
                    self.df_temp_flat_ip = pd.concat([self.df_temp_flat_ip, data], axis=0)
                    self.df_flat_ip = tracking_flat_file(self.df_temp_flat_ip, file_url)

                    if len(processed_data) != 0:
                        self.df_new_key_ip = check_new_key(df_new_key=self.df_new_key_ip, processed_data=processed_data, sector=sector)

                    data = get_project_key(flag_key, processed_data, data, sector, engine)

                    try:
                        result = insert_to_fresh(file_url, data, self.cnt_str)
                        print(colored("insert_to_fresh SUCESSFUL!", 'green'))
                    except Exception as e:
                        print(colored("insert_to_fresh FAILED:", str(e), 'red'))
                else:
                    print(self.df_dict)
                    print(colored('Validate failed', 'red'))
                    pass

    def send_email(self):
        if len(self.df_dict) != 0:
            print(colored('Missing values in dictionary', 'yellow'))
            df_noti_html = convert_df_to_html(type_html=2, df=self.df_dict, cnxn=engine)
            run_email(email_type=2, user_email=to_email, df_noti_html=df_noti_html)

        df_flat_html, df_query_html = convert_df_to_html(type_html=3, list_df=[self.df_flat_ip], type_sector=2, cnxn=engine)
        run_email(type_sector='IP', email_type=3, user_email=to_email, df_flat_html=df_flat_html, df_query_html=df_query_html)

        df_new_key_html = convert_df_to_html(type_html=4, list_df=[self.df_new_key_ip], cnxn=engine)
        if len(df_new_key_html) != 0:
            print(colored('Some new keys were created', 'yellow'))
            run_email(type_sector='IP', email_type=4, user_email=to_email, df_noti_html=df_new_key_html)

    def track_audit(self):
        insert_to_tracking([self.df_flat_ip], 'Tracking_IP', engine)

# Then in your main script:
data = IPData(url, columns_that_need_unidecode=['Project_Name', 'Sub_Project_Name', 'Developer_Name', 'City', 'District', 'Target_Industry'], cnt_str)
data.process_data()
data.send_email()
data.track_audit()
