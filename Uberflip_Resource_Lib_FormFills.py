from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import datetime
# ESTABLISHING CONNECTION WITH THE SNOWFLAKE WAREHOUSE
 

engine = create_engine(URL(
    account='nua76068.us-east-1',
    user='BURHAND',
    password='Core@123',
    database='WH_REPORTING',
    schema='2023Q1',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
))

# read csv data from source file

source_file_loc = (r"C:\Work\Workhuman\2023\Q1\Lloyd\Source\Round 4\OneDrive_2023-03-14\010423_031223_Uber_Flipforms.xlsx")
ext_file_location = (r"C:\Work\Workhuman\2023\Q1\Lloyd\Source\Round 4\Extracted\010423_031223_Uber_Flipforms_updated.csv")


# if you want to read data from csv then uncomment the below line
# df = pd.read_excel(source_file_loc,encoding ='latin1')

# if you want to read data from excel file then uncomment the below line
df = pd.read_excel(source_file_loc,keep_default_na=False)

# function to remove extra space between strings from columns of object data type
# def whitespace_remover(dataframe):
   
#     for i in dataframe.columns:
#         if dataframe[i].dtype != 'object':
#             pass
#         else:
#             dataframe[i] = dataframe[i].str.replace('\s+', '', regex=True)
# whitespace_remover(df)

# create csv from the above data frame

df.to_csv(ext_file_location, encoding='utf-8',  index = False, sep=',')


# read csv data

df = pd.read_csv(ext_file_location,encoding ='latin1',keep_default_na=False)
df.columns=['UTM_SOURCE', 'UTM_MEDIUM','UTM_CAMPAIGN', 'UTM_CONTENT', 'PAGE_VIEW_DATE', 'UTM_FORM_CTA_SUBMISSIONS']
df["PAGE_VIEW_DATE"] = pd.to_datetime(df["PAGE_VIEW_DATE"]).dt.strftime('%Y-%m-%d')

# insert df into snowflake
# Note: Always use lower case letters as table name

print(df)
connection = engine.connect()
df.to_sql('uberflip_resource_lib_formfills', engine, if_exists='replace', index=False, index_label=None, chunksize=None,
dtype={'PAGE_VIEW_DATE': sqlalchemy.Date}, method=None) 
connection.close()
engine.dispose()

print('Data successully imported')
