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

fecha_inicio = input("Ingrese la fecha de inicio (YYYY-MM-DD): ")
fecha_termina = input("Ingrese la fecha de término (YYYY-MM-DD): ")

headers = {
    "Authorization": f"Bearer {api_key}"
}

timeout = httpx.Timeout(
    connect=10.0,
    read=300.0,
    write=30.0,
    pool=30.0
)

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

# recorrido de paginas
async def fetch_page(client, base_url, offset, count, list_id):
    params = {
        "offset": offset,
        "count": count,
        "list_id": list_id,
        "status": "sent",
        "sort_dir": "DESC",
        "before_send_time": fecha_termina,
        "since_send_time": fecha_inicio,
        "fields": "campaigns.id,campaigns.settings.title,campaigns.send_time,campaigns.recipients.list_name,campaigns.settings.from_name,campaigns.emails_sent,campaigns.report_summary.unique_opens,campaigns.report_summary.subscriber_clicks,campaigns.report_summary.opens,campaigns.report_summary.clicks"
    }
    print(f"Fetching page with offset {offset} and count {count}...")
    for intento in range(3):
        try:
            response = await client.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data["campaigns"]
        
        except httpx.HTTPStatusError as e:
            ultimo_error = e

            print(f"Status: {e.response.status_code}")
            print(f"Body: {e.response.text}")

            if intento < 2:
                await asyncio.sleep(10)

        except httpx.RequestError as e:
            ultimo_error = e

            print(f"RequestError: {type(e).__name__}")
            print(f"URL: {e.request.url}")
            print(e)

            if intento < 2:
                await asyncio.sleep(10)

        except Exception as e:
            ultimo_error = e

            print(
                f"offset={offset} "
                f"intento={intento+1} "
                f"error={repr(e)}"
            )

            if intento < 2:
                await asyncio.sleep(10)

    if ultimo_error is not None:
        raise ultimo_error

    raise RuntimeError(f"No se pudo obtener la página de la campaña {campaign_id}")

# obtener cantidad de paginas
async def obtener_total_items(client, url_campaign_all, params):

    for intento in range(3):
        try:
            response = await client.get(
                url_campaign_all,
                headers=headers,
                params=params
            )

            response.raise_for_status()

            return response.json().get("total_items", 0)

        except Exception as e:

            print(
                f"intento {intento+1}/3 "
                f"{type(e).__name__}: {e}"
            )

            if intento == 2:
                raise

            await asyncio.sleep(10)

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

    columnas = [
        "id",
        "nombre",
        "fecha_envio",
        "audiencia",
        "remitente",
        "correos_enviados",
        "aperturas",
        "clicks",
        "total_aperturas",
        "total_clicks",
    ]

    return pd.DataFrame(rows, columns=columnas)

# metric getters
def get_metric(items, key):
    return items.get(key) if isinstance(items, dict) else None

async def campaigns_all(list_id: str, fecha_inicio: str, fecha_termina: str):
    count = 500

    url_campaigns_all = f"{url}/campaigns"

    params_page = {
        "fields": "total_items",
        "list_id": list_id,
        "status": "sent",
        "before_send_time": fecha_termina,
        "since_send_time": fecha_inicio,
    }
    print(f"Parameters for total items: {params_page}")


    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            print("Fetching total items...")

            total_items = await obtener_total_items(
                client,
                url_campaigns_all,
                params_page
            )

            if total_items == 0:
                print(f"No existe campañas para la audiencia {list_id} en el rango de fechas {fecha_inicio} - {fecha_termina}.")
                return None
            
            total_pages = math.ceil(total_items / count)

            print(f"Total items: {total_items}, Total pages: {total_pages}")

            results = []

            for offset in range(0, total_items, count):
                print(f"Consultando offset {offset}")

                page = await fetch_page(
                    client,
                    url_campaigns_all,
                    offset,
                    count,
                    #semaphore,
                    list_id
                )

                results.append(page)

            df_campaigns = build_dataframe(
                results,
                lambda r : {
                    "id": str(r.get("id")) if r.get("id") is not None else None, # id
                    "nombre": get_metric(r.get("settings"), "title"), # campaign_title
                    "fecha_envio": r.get("send_time"), # send_time
                    "audiencia": get_metric(r.get("recipients"), "list_name"), # campaign_title
                    "remitente": get_metric(r.get("settings"), "from_name"), # campaign_title
                    "correos_enviados": r.get("emails_sent"), # emails_sent
                    "aperturas": get_metric(r.get("report_summary"), "unique_opens"), # unique_opens
                    "clicks": get_metric(r.get("report_summary"), "subscriber_clicks"), # subscriber_clicks
                    "total_aperturas": get_metric(r.get("report_summary"), "opens"), # opens
                    "total_clicks": get_metric(r.get("report_summary"), "clicks"), # clicks
                }
            )

            df_campaigns["fecha_envio"] = (
                pd.to_datetime(df_campaigns["fecha_envio"], utc=True)
                .dt.tz_convert("America/Lima")
                .dt.tz_localize(None)
            )

            df_campaigns["codigo_brief"] = df_campaigns["nombre"].str.extract( # type: ignore
            r"(BRI-\d+(?:\s*-\s*[A-Za-z0-9]+)?)",
            expand=False
            ) 

            #df_campaigns.to_csv(f"campaigns_report_{fecha_inicio}_{fecha_termina}.csv", index=False)

            print(f"Total records fetched: {len(df_campaigns)}")


            if df_campaigns.empty:
                print("No hay datos para procesar.")
                return "SIN DATOS"

            return df_campaigns

        except httpx.HTTPStatusError as e:
            print(e.response.status_code)
            print(e.response.text)
            raise

        except httpx.RequestError as e:
            print(e)
            raise

def insert_bd(df_charge):
    try:
        with engine.begin() as connection:

            for i in range(0, len(df_charge), 300):
                try:
                    print(f"Insertando filas {i} - {i+300}")

                    df_charge.iloc[i:i+300].to_sql(
                        "MAILCHIMP_CAMPAIGN",
                        schema="UPAXIS",
                        con=connection,
                        if_exists="append",
                        index=False,
                        method="multi"
                    )
                except DBAPIError as e:
                    print("Error SQL Server:")
                    print(e.orig)
                    raise

                except Exception:
                    traceback.print_exc()
                    raise
    except Exception:
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        df_charge = asyncio.run(campaigns_all(list_id, fecha_inicio, fecha_termina)) # type: ignore
        insert_bd(df_charge)
        print("Proceso completado.")
    except Exception as e:
        traceback.print_exc()