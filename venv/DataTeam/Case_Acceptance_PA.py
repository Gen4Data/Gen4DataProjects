import csv
import pandas as pd
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pyodbc
from six.moves import urllib
from sqlalchemy import create_engine
from datetime import datetime, timedelta
pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 10000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.width', 1000)
import time
import schedule

from azure.storage.blob import BlobClient, BlobServiceClient
import datetime

def job():
    connection_string = "DefaultEndpointsProtocol=https;AccountName=gen4dwstorage;AccountKey=MKUnnILqtVIDTMJuJQ724aO5ldw1fz7FnxQ2aEu+8CUu9de2xIhMvVBQVWtfE03Il+kpBgMdhfsTO4nN3tattw==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("sftptarget")
    blobs_list = container_client.list_blobs()

    # Print the names of all the blobs
    hygiene = []
    new_patients = []
    practice_summary = []
    restorative = []
    tx_existing_patient =[]
    tx_new_patient = []
    case_acceptance_new_pat = []
    case_acceptance_existing_pat = []
    provider_hourly = []
    for blob in blobs_list:
        if 'Gen4ManagementData/Gen4Management_Hygiene' in blob.name:
            hygiene.append(blob.name)
        if 'Gen4ManagementData/Gen4Management_NewPatients' in blob.name:
            new_patients.append(blob.name)
        if 'Gen4ManagementData/Gen4Management_Practice' in blob.name:
            practice_summary.append(blob.name)
        if 'Gen4Management_Restorative' in blob.name:
            restorative.append(blob.name)
            #-------
        if 'Gen4Management_AverageTxPerExisting' in blob.name:
            tx_existing_patient.append(blob.name)
        if 'Gen4Management_AverageTxPerNew' in blob.name:
            tx_new_patient.append(blob.name)
        if 'Gen4Management_CaseAcceptanceperExistingPatient' in blob.name:
            case_acceptance_existing_pat.append(blob.name)
        if 'Gen4Management_CaseAcceptanceperNewPatient' in blob.name:
            case_acceptance_new_pat.append(blob.name)
        if 'Gen4Management_ProviderHours' in blob.name:
            provider_hourly.append(blob.name)




    def sorting(the_list):
        the_list = sorted(the_list,reverse=True)[0]
        return the_list.split()

    hygiene = sorting(hygiene)
    new_patients = sorting(new_patients)
    practice_summary = sorting(practice_summary)
    restorative = sorting(restorative)
    tx_new_patient = sorting(tx_new_patient)
    tx_existing_patient = sorting(tx_existing_patient)
    case_acceptance_new_pat = sorting(case_acceptance_new_pat)
    case_acceptance_existing_pat = sorting(case_acceptance_existing_pat)
    provider_hourly = sorting(provider_hourly)

    # joins them all up together
    updated_files = tx_new_patient + tx_existing_patient + case_acceptance_existing_pat + case_acceptance_new_pat + provider_hourly #hygiene + new_patients + practice_summary + restorative +

    #____________-------------------------------------------------------------#
    #Now I am setup to do a loop
    ##########
    # Send to Azure
    ##########
    server = 'gen4-sql01.database.windows.net'
    database = 'gen4_dw'
    username = 'Dylan'
    password = '8DqGUa536RC7'
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = conn.cursor()
    # cursor.execute('''TRUNCATE TABLE dbo.EOD_Collection_Summary''')
    cursor.commit()
    params = urllib.parse.quote_plus(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
    engine.connect()
    DRIVER = "ODBC Driver 17 for SQL Server"
    #blank_df
    final_df = pd.DataFrame()

    for i in updated_files:


        blob_client = container_client.get_blob_client(i)
        blob_content = blob_client.download_blob().readall().decode("utf-8")
        rows = csv.reader(blob_content.splitlines())
        data = [row for row in rows]
        df = pd.DataFrame(data)
        #grabs column headers
        column_headers = df.iloc[1].str.lower()
        #References everything after the second row is data
        df = df.iloc[2:]
        df.columns = column_headers
        if 'Gen4Management_AverageTxPerExisting' in i:
            df['treatment planning'] = df['treatment planning'].astype(int)
            df['patients'] = df['patients'].astype(int)
            df['year_val'] = df['year_val'].astype(int)
            df['month_val'] = df['month_val'].astype(int)
            df['FirstOfMonth'] = pd.to_datetime(df['year_val'].astype(str) + '-' + df['month_val'].astype(str) + '-01')
            df['provider'] = df['provider'].str.upper()
            df = df.groupby(['office_id', 'office_nm', 'provider', 'year_val', 'month_val', 'FirstOfMonth']).agg(
                {'treatment planning': 'sum', 'patients': 'sum'}).reset_index()
            tx_existing = df
            df.to_sql(name='Gen4_PA_Tx_Existing', schema='dbo', con=engine, index=False, if_exists='replace',
                      method=None, chunksize=200)

        if 'Gen4Management_AverageTxPerNew' in i:
            df['treatment planning'] = df['treatment planning'].astype(int)
            df['patients'] = df['patients'].astype(int)
            df['year_val'] = df['year_val'].astype(int)
            df['month_val'] = df['month_val'].astype(int)
            df['FirstOfMonth'] = pd.to_datetime(df['year_val'].astype(str) + '-' + df['month_val'].astype(str) + '-01')
            df['provider'] = df['provider'].str.upper()
            df = df.groupby(['office_id', 'office_nm', 'provider', 'year_val', 'month_val', 'FirstOfMonth']).agg(
                {'treatment planning': 'sum', 'patients': 'sum'}).reset_index()
            tx_new = df
            df.to_sql(name='Gen4_PA_Tx_New', schema='dbo', con=engine, index=False, if_exists='replace',
                      method=None, chunksize=200)

        if 'Gen4Management_CaseAcceptanceperExistingPatient' in i:
            df['treatment planning'] = df['treatment planning'].astype(int)
            df['patients'] = df['patients'].astype(int)
            df['year_val'] = df['year_val'].astype(int)
            df['month_val'] = df['month_val'].astype(int)
            df['FirstOfMonth'] = pd.to_datetime(df['year_val'].astype(str) + '-' + df['month_val'].astype(str) + '-01')
            df['provider'] = df['provider'].str.upper()
            df = df.groupby(['office_id', 'office_nm', 'provider', 'year_val', 'month_val', 'FirstOfMonth']).agg({'treatment planning':'sum', 'patients':'sum'}).reset_index()
            ca_existing = df
            df.to_sql(name='Gen4_PA_CA_ExistingPat', schema='dbo', con=engine, index=False, if_exists='replace',
                      method=None, chunksize=200)

        if 'Gen4Management_CaseAcceptanceperNewPatient' in i:
            df['treatment planning'] = df['treatment planning'].astype(int)
            df['patients'] = df['patients'].astype(int)
            df['year_val'] = df['year_val'].astype(int)
            df['month_val'] = df['month_val'].astype(int)
            df['FirstOfMonth'] = pd.to_datetime(df['year_val'].astype(str) + '-' + df['month_val'].astype(str) + '-01')
            df['provider'] = df['provider'].str.upper()
            df = df.groupby(['office_id', 'office_nm', 'provider', 'year_val', 'month_val', 'FirstOfMonth']).agg(
                {'treatment planning': 'sum', 'patients': 'sum'}).reset_index()
            ca_new = df
            df.to_sql(name='Gen4_PA_CA_NewPat', schema='dbo', con=engine, index=False, if_exists='replace',
                      method=None, chunksize=200)

        if 'Gen4Management_ProviderHours' in i:
            df['year_val'] = df['year_val'].astype(int)
            df['month_val'] = df['month_val'].astype(int)
            df['hours_worked'] = df['hours_worked'].astype(int)
            df['FirstOfMonth'] = pd.to_datetime(df['year_val'].astype(str) + '-' + df['month_val'].astype(str) + '-01')
            df['provider'] = df['provider'].str.upper()
            # df = df.groupby(['office_id', 'office_nm', 'provider', 'year_val', 'month_val', 'FirstOfMonth']).agg(
            #     {'treatment planning': 'sum', 'patients': 'sum'}).reset_index()
            df.to_sql(name='Gen4_PA_provider_hourly', schema='dbo', con=engine, index=False, if_exists='replace',
                      method=None, chunksize=200)



    import re
    def remove_punctuation(text):
        # Regular expression to match punctuation characters
        punctuation = re.compile('[^\w\s]')
        # Replace punctuation characters with an empty string
        return punctuation.sub('', text)



    case_acceptance = pd.merge(ca_new, ca_existing, how='outer',left_on=['office_id', 'office_nm', 'provider', 'year_val', 'month_val', 'FirstOfMonth'], right_on=['office_id', 'office_nm', 'provider', 'year_val', 'month_val','FirstOfMonth'])
    case_acceptance = case_acceptance.rename(columns={'treatment planning_x':'treatment_planning_new', 'patients_x':'patients_new', 'treatment planning_y':'treatment_planning_existing', 'patients_y':'patients_existing'})
    case_acceptance = case_acceptance.fillna(0)
    case_acceptance['treatment_planning'] = case_acceptance['treatment_planning_new'] + case_acceptance['treatment_planning_existing']
    case_acceptance['patients'] = case_acceptance['patients_new'] + case_acceptance['patients_existing']
    case_acceptance.loc[case_acceptance['treatment_planning_new'] > 0, 'new_patients_bool'] = 1
    case_acceptance.loc[case_acceptance['treatment_planning_existing'] > 0, 'existing_patients_bool'] = 1
    case_acceptance['new_patients_bool'] = case_acceptance['new_patients_bool'].fillna(0)
    case_acceptance['existing_patients_bool'] = case_acceptance['existing_patients_bool'].fillna(0)
    # case_acceptance['pk'] = case_acceptance['office_id'] & case_acceptance['provider'] & case_acceptance['year_val'].apply(str) & case_acceptance['month_val'].apply(str)
    # df['pk'] = case_acceptance.apply(lambda row: str(case_acceptance['office_id']) + str(case_acceptance['provider']) + str(case_acceptance['year_val']) + str(case_acceptance['month_val']), axis=1)
    case_acceptance['provider'] = case_acceptance['provider'].apply(lambda x: ' '.join(x.split(', ')[::-1]))

    case_acceptance['provider'] = case_acceptance['provider'].apply(remove_punctuation)
    case_acceptance['provider'] = case_acceptance['provider'].str.strip()

    case_acceptance = case_acceptance.groupby(['office_id', 'office_nm', 'provider', 'year_val', 'month_val',
                                                     'FirstOfMonth']).agg({'treatment_planning_new':'sum',
                                                                           'patients_new':'sum',
                                                                           'treatment_planning_existing':'sum',
                                                                           'patients_existing':'sum',
                                                                           'treatment_planning':'sum',
                                                                           'patients':'sum',
                                                                           'new_patients_bool':'sum',
                                                                           'existing_patients_bool':'sum'}).reset_index()

    case_acceptance.to_sql(name='Gen4_PA_Case_Acceptance', schema='dbo', con=engine, index=False, if_exists='replace',
                      method=None, chunksize=200)
    print(case_acceptance.head(10))



    treatment_planning = pd.merge(tx_new, tx_existing, how='outer',left_on=['office_id', 'office_nm', 'provider', 'year_val', 'month_val', 'FirstOfMonth'], right_on=['office_id', 'office_nm', 'provider', 'year_val', 'month_val','FirstOfMonth'])

    treatment_planning = treatment_planning.rename(columns={'treatment planning_x':'treatment_planning_new', 'patients_x': 'patients_new', 'treatment planning_y': 'treatment_planning_existing', 'patients_y':'patients_existing'})
    treatment_planning = treatment_planning.fillna(0)
    treatment_planning['treatment_planning'] = treatment_planning['treatment_planning_new'] + treatment_planning['treatment_planning_existing']
    treatment_planning['patients'] = treatment_planning['patients_new'] + treatment_planning['patients_existing']
    treatment_planning['provider'] = treatment_planning['provider'].apply(lambda x: ' '.join(x.split(', ')[::-1]))


    treatment_planning['provider'] = treatment_planning['provider'].apply(remove_punctuation)
    treatment_planning['provider'] = treatment_planning['provider'].str.strip()
    treatment_planning = treatment_planning.groupby(['office_id', 'office_nm', 'provider', 'year_val', 'month_val',
                                                     'FirstOfMonth']).agg({'treatment_planning_new':'sum',
                                                                           'patients_new':'sum',
                                                                           'treatment_planning_existing':'sum',
                                                                           'patients_existing':'sum',
                                                                           'treatment_planning':'sum',
                                                                           'patients':'sum'}).reset_index()


    treatment_planning.to_sql(name='Gen4_PA_Treatment_Planning', schema='dbo', con=engine, index=False, if_exists='replace',
                      method=None, chunksize=200)

schedule.every().hour.do(job)
# schedule.every(300).seconds.do(job)
while True:
    schedule.run_pending()
    time.sleep(1)