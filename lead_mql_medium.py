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

source_file_loc = (r"C:\Work\Workhuman\2023\Q1\Lloyd\Source\Round 2\OneDrive_1_2-13-2023\010423_021223_Transmission_Quicksight_Leads.xlsx")
ext_file_location = (r"C:\Work\Workhuman\2023\Q1\Lloyd\Source\Round 2\Extracted\010423_021223_Transmission_Quicksight_Leads_updated.csv")


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
df.columns=['RESPONSE_DATE', 'CAMPAIGN_NAME', 'TOTAL_CAMPAIGN_MEMBERS', 'TOTAL_POST_RESPONSE_MQLS']
df["RESPONSE_DATE"] = pd.to_datetime(df["RESPONSE_DATE"]).dt.strftime('%Y-%m-%d')

# insert df into snowflake
# Note: Always use lower case letters as table name

print(df)
connection = engine.connect()
df.to_sql('transmission_quicksight_leads', engine, if_exists='replace', index=False, index_label=None, chunksize=None,
dtype={'RESPONSE_DATE': sqlalchemy.Date}, method=None) 
connection.close()
engine.dispose()

print('Data successully imported')
