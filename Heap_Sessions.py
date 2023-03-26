from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
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


source_file_loc = (r"C:\Work\Workhuman\2023\Q1\Lloyd\Source\Round 4\OneDrive_2023-03-14\010423_031223_Acquisition_Heap_Sessions.xlsx")
ext_file_location = (r"C:\Work\Workhuman\2023\Q1\Lloyd\Source\Round 4\Extracted\010423_031223_Acquisition_Heap_Sessions_updated.csv")

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

df = pd.read_csv(ext_file_location,keep_default_na=False)
df.columns=['UTM_CAMPAIGN_HS', 'UTM_SOURCE_HS', 'UTM_MEDIUM_HS', 'UTM_CONTENT_HS', 'VISITS_HS']


# insert df into snowflake
# Note: Always use lower case letters as table name. No matter what
print(df)
connection = engine.connect()
df.to_sql('acquisition_heap_sessions', engine, if_exists='replace', index=False, index_label=None, chunksize=None, dtype=None, method=None) 
connection.close()
engine.dispose()

print('Data successully imported')
