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
async def fetch_page(client, base_url, offset, count, semaphore):
    async with semaphore:
        params = {
            "offset": offset,
            "count": count,
            #"since_send_time": "2026-07-08T19:50:00+00:00",
        }
        print(f"Fetching page with offset {offset} and count {count}...")
        for intento in range(3):
            try:
                response = await client.get(base_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                return data["tags"]
            
            except httpx.ReadTimeout:
                print(f"Timeout. Reintentando ({intento + 1}/3)...")
                await asyncio.sleep(5)
                
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

async def tags_all(list_id: str):
    count = 1000

    url_tags_all = f"{url}/lists/{list_id}/tag-search"

    params_page = {
        "fields": "total_items",
        #"since_send_time": "2026-07-08T19:50:00+00:00",
    }

    semaphore = asyncio.Semaphore(2)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:

            print("Fetching total items...")
            response = await client.get(url_tags_all, headers=headers, params=params_page)
            response.raise_for_status()
            total_items = response.json().get("total_items", 0)

            total_pages = math.ceil(total_items / count)

            print(f"Total items: {total_items}, Total pages: {total_pages}")

            results = []

            for offset in range(0, total_items, count):
                print(f"Consultando offset {offset}")

                page = await fetch_page(
                    client,
                    url_tags_all,
                    offset,
                    count,
                    semaphore
                )

                results.append(page)

            df_tags = build_dataframe(
                results,
                lambda r : {
                    "tag_id": str(r.get("id")) if r.get("id") is not None else None, # id
                    "nombre_tag": r.get("name"), # campaign_title
                }
            )

            print(f"Total records fetched: {len(df_tags)}")

            if df_tags.empty:
                print("No hay datos para procesar.")
                return "SIN DATOS"
            

            df_tags.to_csv("tags_report.csv", index=False)

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

                    connection.execute(text("DELETE FROM UPAXIS.MAILCHIMP_TAGS"))
                    for i in range(0, len(df_tags), 1000):
                        try:
                            print(f"Insertando filas {i} - {i+1000}")

                            df_tags.iloc[i:i+1000].to_sql(
                                "MAILCHIMP_TAGS",
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

            except Exception as e:
                traceback.print_exc()


            return "LISTO!"
        except httpx.HTTPStatusError as e:
            print(e.response.status_code)
            print(e.response.text)

        except httpx.RequestError as e:
            print(e)


if __name__ == "__main__":
    try:
        asyncio.run(tags_all(list_id)) # type: ignore
        print("Proceso completado.")
    except Exception as e:
        traceback.print_exc()