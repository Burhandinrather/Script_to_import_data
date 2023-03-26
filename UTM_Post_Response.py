from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import datetime
import numpy as np

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

source_file_loc = (r"C:\Work\Workhuman\2023\Q1\Lloyd\Source\Round 4\OneDrive_2023-03-14\UTM_Post_Response_Data thru 3.12.xlsx")
ext_file_location = (r"C:\Work\Workhuman\2023\Q1\Lloyd\Source\Round 4\Extracted\UTM_Post_Response_Data thru 3.12_updated.csv")

# if you want to read data from csv then uncomment the below line
#df = pd.read_csv(source_file_loc,encoding ='latin1')

# if you want to read data from excel file then uncomment the below line
df = pd.read_excel(source_file_loc,keep_default_na=False)

# create csv from the above data frame
print (df)
df.to_csv(ext_file_location, encoding='utf-8',  index = False, sep=',')


# read csv data

df = pd.read_csv(ext_file_location,keep_default_na=False)
df.columns=['SFPERSON_ID', 'UTM_CAMPAIGN', 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CONTENT', 'RESPONSE_DATE', 'MQL_DATE', 'POST_REPSONSE_MQL']

df["RESPONSE_DATE"] = df["RESPONSE_DATE"].replace("null", np.nan)
df["RESPONSE_DATE"] = pd.to_datetime(df["RESPONSE_DATE"]).dt.strftime('%Y-%m-%d')
df["RESPONSE_DATE"] = df["RESPONSE_DATE"].fillna(pd.Timestamp('nat'))

df["MQL_DATE"] = df["MQL_DATE"].replace("null", np.nan)
df["MQL_DATE"] = pd.to_datetime(df["MQL_DATE"]).dt.strftime('%Y-%m-%d')
df["MQL_DATE"] = df["MQL_DATE"].fillna(pd.Timestamp('nat'))


# insert df into snowflake
# Note: Always use lower case letters as table name. No matter what
print(df)
connection = engine.connect()
df.to_sql('utm_post_response_mq', engine, if_exists='replace', index=False, index_label=None, chunksize=None, 
dtype={'RESPONSE_DATE': sqlalchemy.Date, 'MQL_DATE': sqlalchemy.Date}, method=None) 
connection.close()
engine.dispose()

print('Data successully imported')
