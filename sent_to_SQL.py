import pandas as pd
import os
import urllib.parse
import asyncio
import httpx
from dotenv import load_dotenv
import sys
import os
import math
from typing import List, Dict, Any
import pyodbc
from sqlalchemy import create_engine, text
import traceback
from sqlalchemy.exc import SQLAlchemyError,DBAPIError

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

with engine.begin() as connection:
    df_ids = pd.read_sql_query("SELECT top 10 id FROM UPAXIS.MAILCHIMP_CAMPAIGN where convert(nvarchar(6),fecha_envio,112) between '202601' and '202612' order by 1", connection)

df_ids.to_csv("campaign_ids.csv", index=False)

headers = {
    "Authorization": f"Bearer {api_key}"
}

timeout = httpx.Timeout(
    connect=10.0,
    read=300.0,
    write=30.0,
    pool=30.0
)

# recorrido de paginas
async def fetch_page(client, base_url, offset, count):
    #async with semaphore:
    params = {
        "offset": offset,
        "count": count,
        "fields": "sent_to.email_address,sent_to.campaign_id,sent_to.status,sent_to.open_count,sent_to.last_open"
    }
    print(f"Fetching page with offset {offset} and count {count}...")
    for intento in range(3):
        try:
            response = await client.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data["sent_to"]
        
        except httpx.ReadTimeout:
            print(f"Timeout. Reintentando ({intento + 1}/3)...")
            await asyncio.sleep(10)
            
        except httpx.HTTPStatusError as e:
            print(f"HTTP {e.response.status_code} en offset={offset}")

        except httpx.RequestError as e:
            print(type(e))
            print(e)

# dataframe builder
def build_dataframe(results, mapper):
    rows = []

    for page in results:
        if isinstance(page, Exception):
            raise page  # no ocultar errores

        if not isinstance(page, list):
            continue

        for r in page:
            rows.append(mapper(r))

    return pd.DataFrame(rows)

async def sent_to_all(campaign_id: str):
    count = 1000

    url_sent_to_all = f"{url}/reports/{campaign_id}/sent-to"

    params_page = {        
        "fields": "total_items",
    }

    #print(f"Parameters for total items: {params_page}")

    #semaphore = asyncio.Semaphore(2)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            print("Fetching total items...")
            response = await client.get(url_sent_to_all, headers=headers, params=params_page)
            response.raise_for_status()
            total_items = response.json().get("total_items", 0)

            total_pages = math.ceil(total_items / count)

            print(f"Total items: {total_items}, Total pages: {total_pages}")

            results = []

            for offset in range(0, total_items, count):
                print(f"Consultando offset {offset}")

                page = await fetch_page(
                    client,
                    url_sent_to_all,
                    offset,
                    count,
                    #semaphore
                )
                results.append(page)
            
            df_sent_to = build_dataframe(
                results,
                lambda r : {
                    "email": str(r.get("email_address")) if r.get("email_address") is not None else None, # id
                    "idcampaign": r.get("campaign_id"), # campaign_title
                    "status": r.get("status"), # send_time
                    "aperturas": r.get("open_count"), # send_time
                    "Ultima_apertura": r.get("last_open") or None, # send_time
                }
            )

            df_sent_to["Ultima_apertura"] = (
                pd.to_datetime(
                    df_sent_to["Ultima_apertura"],
                    errors="coerce",
                    utc=True
                )
                .dt.tz_convert("America/Lima")
                .dt.tz_localize(None)
            )

            df_sent_to["email"] = df_sent_to["email"].str.lower()

            #df_sent_to.to_csv(f"sent_to_{campaign_id}.csv", index=False)

            print(f"Total records fetched: {len(df_sent_to)}")

            if df_sent_to.empty:
                print("No hay datos para procesar.")
                return None

            return df_sent_to

        except httpx.HTTPStatusError as e:
            print(e.response.status_code)
            print(e.response.text)

        except httpx.RequestError as e:
            print(e)

def insert_bd(df_charge):
    try:
        with engine.begin() as connection:

            #connection.execute(text("DELETE FROM UPAXIS.MAILCHIMP_CAMPAIGN"))
            for i in range(0, len(df_charge), 300):
                try:
                    print(f"Insertando filas {i} - {i+300}")

                    df_charge.iloc[i:i+300].to_sql(
                        "MAILCHIMP_EMAILS_CAMPAIGN",
                        schema="UPAXIS",
                        con=connection,
                        if_exists="append",
                        index=False,
                        method="multi"
                    )
                except DBAPIError as e:
                    print("Error SQL Server:")
                    print(e.orig)

                except Exception:
                    traceback.print_exc()

    except Exception as e:
        traceback.print_exc()


if __name__ == "__main__":
    try:
        dfs_final = []
        for i, campaign_id in enumerate(df_ids["id"], start=1):
            print(f"Procesando campaña {i}: {campaign_id}")
            df = asyncio.run(sent_to_all(campaign_id))

            if df is not None:
                dfs_final.append(df)

            # Cada 20 campañas
            if i % 20 == 0:
                df_charge = pd.concat(dfs_final, ignore_index=True)
                insert_bd(df_charge)
                dfs_final = []

        # Inserta las restantes
        if dfs_final:
            df_charge = pd.concat(dfs_final, ignore_index=True)
            insert_bd(df_charge)
        print("Proceso completado.")
    except Exception as e:
        traceback.print_exc()