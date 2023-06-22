from lib import *


#Connnect SQL Server 
def ConnectAzureSQLServer(): 
    f = open ('database_information.json', "r") #Database information file, can change information depending on the time
    qq = json.loads(f.read())
    f.close()
    ini_cnt_str ='Driver={driver_str};Server=tcp:{servername},1433;database={database};Uid={username};Pwd={password};Encrypt=yes;Authentication=ActiveDirectoryPassword;Connection Timeout=30;'.format(**qq)
    quoted = quote_plus(ini_cnt_str)
    cnt_str = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(cnt_str)

        #Test Connection
    try:
        conn = engine.connect()
        result = conn.execute(text("SELECT 1"))
        print("CONNECTION SUCESSFUL!")
    except Exception as e:
        print("CONNECTION FAILED:",str(e))
    return cnt_str, engine
# Connect SharePoint Get FlatFile
#url_hub= input(str) Khi xay dung he thong co nhieu hub thi thay doi kieu input
def ConnectSharePoint(url_hub):
    header_Hub = f"share_point_{url_hub.split('/')[2].replace('-','')}"
    config_Hub = read_config_json(config_path, header_Hub)
    Hub = SharePoint(config_Hub)
    #Hub.check_connect()
    return Hub
