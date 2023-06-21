from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.client_request_exception import ClientRequestException
import datetime
import pytz
import pandas as pd
import numpy as np
import csv
import glob
import pyodbc
import os 
import json
from io import BytesIO
import io
import platform
from function.PyToSp import *
from tqdm import tqdm
from urllib.parse import quote_plus
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, event,text
import pyodbc
import requests
import inspect
from validate import *
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
from urllib.parse import quote_plus
import msal
from itertools import chain
from send_email import *
import json
