import pandas as pd
import os
from dotenv import load_dotenv
import urllib.parse
import pyodbc
from sqlalchemy import create_engine, text
import traceback
from sqlalchemy.exc import SQLAlchemyError,DBAPIError

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
    
file_path = f"G:\\Proyectos_UP\\conector_Mailchimp_UPAXIS\\mailchimp_report.csv"  # Replace with your actual file path
df_mailchimp = read_data_from_csv(file_path)

# print(df_limpio.shape)
# print(df_limpio.head())

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

        connection.execute(text("DELETE FROM UPAXIS.MAILCHIMP_BRIEF"))

        for i in range(0, len(df_mailchimp), 100): # type: ignore
            try:
                print(f"Insertando filas {i} - {i+100}")

                df_mailchimp.iloc[i:i+100].to_sql( # type: ignore
                    "MAILCHIMP_BRIEF",
                    schema="UPAXIS",
                    con=connection,
                    if_exists="append",
                    index=False
                )
            except DBAPIError as e:
                print("Error SQL Server:")
                print(e.orig)

            except Exception as e:
                print("Error general:")
                print(e)
                break

except DBAPIError as e:
    print("Error SQL Server:")
    print(e.orig)

except Exception as e:
    print("Error general:")
    print(e)

#print(df_limpio.head(5))  # Display the first few rows of the DataFrame 