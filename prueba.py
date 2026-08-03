from dotenv import load_dotenv
import sys
import os
import urllib.parse
import pyodbc
from sqlalchemy import create_engine, text
import pandas as pd

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

load_dotenv(os.path.join(BASE_DIR, ".env"))

api_key = os.getenv("API_KEY")
url = os.getenv("URL")
list_id = os.getenv("id_audience")

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

headers = {
    "Authorization": f"Bearer {api_key}"
}

with engine.begin() as connection:
    query = text("SELECT MAX(fecha_envio) FROM UPAXIS.MAILCHIMP_CAMPAIGN")
    resultado = connection.execute(query).fetchone()
    fecha_inicio = resultado[0] if resultado else None

if fecha_inicio is not None:
    fecha_inicio = (
        pd.Timestamp(fecha_inicio)
        .tz_localize("America/Lima")  # UTC-5
        .tz_convert("UTC")            # UTC+0
        .isoformat(timespec="seconds")
    )

fecha_termina = (
    pd.Timestamp.now(tz="UTC")
    .isoformat(timespec="seconds")
)

print(f"Fecha de inicio: {fecha_inicio}")
print(f"Fecha de término: {fecha_termina}")