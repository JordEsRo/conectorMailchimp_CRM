import pandas as pd
import os
from dotenv import load_dotenv
import urllib.parse
import pyodbc
from sqlalchemy import create_engine, text

load_dotenv()

def read_data_from_csv(file_path):
    """
    Reads data from a CSV file and returns it as a pandas DataFrame.

    Parameters:
    file_path (str): The path to the CSV file.

    Returns:
    pd.DataFrame: DataFrame containing the data from the CSV file.
    """
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None
    
file_path = f"C:\\Users\\jorda\\Downloads\\66538465_reports\\campaigns.csv"  # Replace with your actual file path
df = read_data_from_csv(file_path)

obj_cols = df.select_dtypes(include=["object", "string"]).columns # type: ignore
df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip()) # type: ignore

df['codigo_brief'] = df['Title'].str.extract(r'(BRI-\d+)')  # Extract numeric code from the 'Title' column # type: ignore

df_limpio = df[df["codigo_brief"].notna()].copy() # type: ignore

df_limpio = df_limpio.rename(columns={
    "Title": "title",
    "Subject": "subject",
    "Audience": "audience",
    "Send Date": "send_date",
    "Send Weekday": "send_weekday",
    "Total Recipients": "total_recipients",
    "Successful Deliveries": "successful_deliveries",
    "Soft Bounces": "soft_bounces",
    "Hard Bounces": "hard_bounces",
    "Total Bounces": "total_bounces",
    "Times Forwarded": "times_forwarded",
    "Forwarded Opens": "forwarded_opens",
    "Unique Opens": "unique_opens",
    "Open Rate": "open_rate",
    "Total Opens": "total_opens",
    "Unique Clicks": "unique_clicks",
    "Click Rate": "click_rate",
    "Total Clicks": "total_clicks",
    "Unsubscribes": "unsubscribes",
    "Abuse Complaints": "abuse_complaints",
    "Times Liked on Facebook": "times_liked_facebook",
    "Folder Id": "folder_id",
    "Unique Id": "unique_id",
    "Total Orders": "total_orders",
    "Total Gross Sales": "total_gross_sales",
    "Total Revenue": "total_revenue",
})

for col in ["open_rate", "click_rate"]:
    df_limpio[col] = (
        df_limpio[col]
        .str.rstrip("%")
        .astype(float)
        / 100
    )

df_limpio["send_date"] = pd.to_datetime(
    df_limpio["send_date"],
    format="%b %d, %Y %I:%M %p",
    errors="coerce"
)

df_limpio.to_csv('campaigns_with_codes.csv', index=False)  # Save the DataFrame to a new CSV file

print(df_limpio.shape)
print(df_limpio.head())

server = os.getenv('DB_SERVER') 
database = os.getenv('DB_NAME') 
username = os.getenv('DB_USER') 
password = os.getenv('DB_PASSWORD') 
connection_string = ( 
    f'DRIVER={{ODBC Driver 17 for SQL Server}};' 
    f'SERVER={server};' 
    f'DATABASE={database};' 
    f'UID={username};' 
    f'PWD={password};'
    'Encrypt=no;'
    'TrustServerCertificate=yes;' 
)
params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


try:
    with engine.begin() as connection:

        connection.execute(text("DELETE FROM UPAXIS.UP_BRIEF_MAILCHIMP"))
        df_limpio.to_sql(
            'UP_BRIEF_MAILCHIMP',
            schema="UPAXIS",
            con=connection,
            index=False,
            if_exists='append'
        )

except Exception as e:
    print(f"Error inserting data into the database: {e}")



#print(df_limpio.head(5))  # Display the first few rows of the DataFrame 