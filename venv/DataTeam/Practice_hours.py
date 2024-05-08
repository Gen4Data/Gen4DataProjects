import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import schedule
import time
import scripts
from datetime import datetime

filepath = r"C:\Users\Administrator\OneDrive - Gen4 Dental Partners\Practice Hours\Practice Hours.xlsx"


def job():
    # Read Excel file
    df = pd.read_excel(filepath, skiprows=1)

    scripts.write_to_azure(df, 'RawPracticeHours')
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Job ran successfully at:", timestamp)


# Run the job immediately when the script starts
job()

# Schedule the job to run every 1 hours
schedule.every(1).hours.do(job)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)
