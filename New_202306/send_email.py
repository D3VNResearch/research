import logging
#import azure.functions as func
import datetime
import logging
import json
import msal
import requests
import pandas as pd
from datetime import datetime

'''
email_type = 1 -> duplicate
email_type = 2 -> missing dictionary
email_type = 3 -> tracking audit 
email_type = 4 -> new key
'''

def run_email(type_sector = None, email_type = None, user_email = None, df_flat_html = None, df_query_html = None, df_noti_html = None):
    client_id = 'd66b4e6d-9bfd-4a8e-b191-be8a64c80176'
    client_secret = 'vJB8Q~IGm9gqazcuQBs2EIpkWuLzdfjSdjNbdahs'
    tenant_id = 'd929b288-ebb1-49e2-8435-99994c5663d6'
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(client_id=client_id, client_credential=client_secret, authority=authority)
    scopes = ["https://graph.microsoft.com/.default"]
    result = app.acquire_token_for_client(scopes=scopes)
    result = app.acquire_token_silent(scopes = scopes, account=None)
    email_type = int(email_type)
    list_email_format = []
    for i in user_email:
        list_email_format.append({'EmailAddress': {'Address': i}})
    design_table = "<style>\
                    table, th, td {\
                      border: 1px solid black;\
                      border-collapse: collapse;\
                    }\
                    th, td {\
                      padding-top: 10px;\
                      padding-bottom: 10px;\
                      padding-left: 10px;\
                      padding-right: 10px;\
                    }\
                    th {\
                      text-align: left;\
                      color: white;\
                      background: #789BB8;\
                    }\
                    </style>"
    
    if email_type == 1:
        subject_email = 'DUPLICATE SUB NAME - {} ({})'.format(type_sector, datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        content_email = '''\
                    <p>Hi team,<br>\
                    Please quick check duplicate names in the table below.<br>\
                    Ariga thanks and nice {}!</p>\
                    '''.format(datetime.today().strftime("%A"))\
                    + design_table \
                    + f'{df_noti_html}' \
                    + design_table \

    elif email_type == 2:
        subject_email = 'DICTIONARY - MISSING VALUES ({})'.format(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        content_email = '''\
                    <p>Hi bro,<br>\
                    Please add these new values to dictionary in database.<br>\
                    Ariga thanks and nice {}!</p>\
                    '''.format(datetime.today().strftime("%A"))\
                    + design_table \
                    + f'{df_noti_html}' \
                    + design_table \
            
    #Import data
    elif email_type == 3:
        subject_email = 'IMPORT DATA - {} in {}'.format(type_sector, datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        content_email = '''\
                        <p>Hi team,<br>\
                        Data were imported successfully. <br>Please check database record section below. \
                        If any cells has <span style="background: #E85A24; color: white">orange color</span>, \
                        its probably something wrong here. So, ping dev team immediately!<br>
                        Ariga thanks and happy freakinggg {}!!!</p>\
                        '''.format(datetime.today().strftime("%A"))\
                        + '''<i><b><p style="font-size:120%; color: #000000"> Database record </i></b></p>''' \
                        + design_table \
                        + f'{df_query_html}' \
                        + '''<i><b><p style="font-size:120%; color: #000000"> Flat file record </i></b></p>''' \
                        + f'{df_flat_html}' \
                        + design_table \
                        
    elif email_type == 4:
        subject_email = 'LIST NEW KEY - {} ({})'.format(type_sector, datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        content_email = '''\
                    <p>Hi team,<br>\
                    Please quick view list of new key has been created recently in the table below.<br>\
                    Ariga thanks and nice {}!</p>\
                    '''.format(datetime.today().strftime("%A"))\
                    + design_table \
                    + f'{df_noti_html}' \
                    + design_table \
                    
    try:
        userId = "D3VNResearch@savills.com.vn"
        endpoint = f'https://graph.microsoft.com/v1.0/users/{userId}/sendMail'
        #toUserEmail = "nngocphuonguyen@savills.com.vn"
        email_msg = {'Message': {'Subject': subject_email,
                                 'Body': {'ContentType': 'HTML', 
                                          'Content': content_email
                                        },
                                 'ToRecipients': list_email_format
                                 },
                     'SaveToSentItems': 'true'}
        r = requests.post(endpoint,
                          headers={'Authorization': 'Bearer ' + result['access_token']}, json=email_msg)
        if r.ok:
            print('Sent email successfully')
        else:
            print(r.json())
    except Exception as e:
        print("Error: ", e)


"""def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    email_type = req.params.get('email_type')
    user_email = req.params.get('user_email')
    df_flat_html = req.params.get('df_flat_html')
    df_query_html = req.params.get('df_query_html')
    if not account_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            email_type = req.params.get('email_type')
            user_email = req.params.get('user_email')
            df_flat_html = req.params.get('df_flat_html')
            df_query_html = req.params.get('df_query_html')

    if account_id:
        run_email(email_type, user_email, df_flat_html, df_query_html)
        return func.HttpResponse(f"This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )"""