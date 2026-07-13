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
import time

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
    df_ids = pd.read_sql_query("" \
    "SELECT id " \
    "FROM UPAXIS.MAILCHIMP_CAMPAIGN " \
    "where convert(nvarchar(6),fecha_envio,112) between '202501' and '202512' "
    "and estado IN ('P','X','E') " \
    "order by fecha_envio desc", connection)

print(f"Cantidad de ids a procesar...",{len(df_ids)})

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
async def fetch_page(client, base_url, offset, count, campaign_id):
    #async with semaphore:
    params = {
        "offset": offset,
        "count": count,
        "fields": "sent_to.email_address,sent_to.campaign_id,sent_to.status,sent_to.open_count,sent_to.last_open"
    }
    print(f"Fetching page for campaing {campaign_id} with offset {offset} and count {count}...")

    ultimo_error = None

    for intento in range(3):
        try:
            response = await client.get(base_url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()

            return data["sent_to"]

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
                f"Campaña {campaign_id} "
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
async def obtener_total_items(client, url_sent_to_all,campaign_id):

    params = {
        "fields": "total_items"
    }

    for intento in range(3):
        try:
            response = await client.get(
                url_sent_to_all,
                headers=headers,
                params=params
            )

            response.raise_for_status()

            return response.json().get("total_items", 0)

        except Exception as e:

            print(
                f"Error obteniendo total_items de {campaign_id} "
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
        "email",
        "idcampaign",
        "status",
        "aperturas",
        "Ultima_apertura"
    ]

    return pd.DataFrame(rows, columns=columnas)

async def sent_to_all(campaign_id: str):
    count = 1000

    url_sent_to_all = f"{url}/reports/{campaign_id}/sent-to"

    #print(f"Parameters for total items: {params_page}")

    #semaphore = asyncio.Semaphore(2)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            print("Fetching total items...")
            total_items = await obtener_total_items(
                client,
                url_sent_to_all,
                campaign_id
            )

            if total_items == 0:
                print(f"Campaña {campaign_id}: no tiene destinatarios.")
                return None

            total_pages = math.ceil(total_items / count) # type:ignore

            print(f"Total items: {total_items}, Total pages: {total_pages}")

            results = []

            for offset in range(0, total_items, count): # type:ignore
                print(f"Consultando offset {offset}")

                page = await fetch_page(
                    client,
                    url_sent_to_all,
                    offset,
                    count,
                    campaign_id
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

            if df_sent_to.empty:
                print("No hay datos para procesar.")
                return df_sent_to


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

            print(
                campaign_id,
                total_items,
                len(df_sent_to)
            )

            return df_sent_to

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
                    raise

                except Exception:
                    traceback.print_exc()
                    raise
    except Exception:
        traceback.print_exc()
        raise


def marcar_en_proceso(campaign_id, connection):

    query = text("""
        UPDATE UPAXIS.MAILCHIMP_CAMPAIGN
        SET
            estado = 'E',
            mensaje_error = NULL
        WHERE id = :id
    """)

    connection.execute(query, {"id": campaign_id})

def marcar_error(campaign_id, error, connection):

    query = text("""
        UPDATE UPAXIS.MAILCHIMP_CAMPAIGN
        SET
            estado = 'X',
            mensaje_error = :error
        WHERE id = :id
    """)

    connection.execute(
        query,
        {
            "id": campaign_id,
            "error": str(error)[:500]
        }
    )

def confirmar_lote(campañas_lote, connection):

    query = text("""
        UPDATE UPAXIS.MAILCHIMP_CAMPAIGN
        SET
            estado = 'C',
            fecha_actua_miembros = GETDATE(),
            mensaje_error = NULL
        WHERE id = :id
    """)

    for campaign_id in campañas_lote:
        print(f"Confirmando {campaign_id}")
        connection.execute(query, {"id": campaign_id})
    print("Confirmación terminada")


sem = asyncio.Semaphore(3)

async def procesar_campania(campaign_id):
    async with sem:
        try:
            print(f"Procesando campaña {campaign_id}")

            with engine.begin() as connection:
                marcar_en_proceso(campaign_id, connection)

            df = await sent_to_all(campaign_id)

            return campaign_id, df, None

        except Exception as e:
            traceback.print_exc()

            with engine.begin() as connection:
                marcar_error(campaign_id, e, connection)

            return campaign_id, None, e


async def main_async():

    TOTAL_FILAS = 0
    dfs_final = []
    campañas_lote = []

    tareas = [
        asyncio.create_task(procesar_campania(campaign_id))
        for campaign_id in df_ids["id"]
    ]

    procesadas = 0

    for tarea in asyncio.as_completed(tareas):

        try:
            campaign_id, df, error = await tarea

            procesadas += 1

            if procesadas % 50 == 0:
                print("Esperando 20 segundos...")
                await asyncio.sleep(20)

            if error:
                continue

            if df is None or df.empty:
                with engine.begin() as connection:
                    marcar_error(
                        campaign_id,
                        "No se recuperaron destinatarios",
                        connection
                    )
                    continue

            dfs_final.append(df)
            campañas_lote.append(campaign_id)
            TOTAL_FILAS += len(df)

            if TOTAL_FILAS >= 50000:

                print(f"Insertando {TOTAL_FILAS} registros...")

                df_charge = pd.concat(dfs_final, ignore_index=True)

                insert_bd(df_charge)

                with engine.begin() as connection:
                    confirmar_lote(campañas_lote, connection)

                dfs_final = []
                campañas_lote = []
                TOTAL_FILAS = 0

        except Exception:
            traceback.print_exc()

    # Inserta lo pendiente
    if dfs_final:

        df_charge = pd.concat(dfs_final, ignore_index=True)

        print("Insertando lote...con:", len(df_charge),"elementos")
        insert_bd(df_charge)
        print("Insert terminado...con:", len(df_charge),"elementos")

        with engine.begin() as connection:
            confirmar_lote(campañas_lote, connection)

    print("Proceso completado.")


# main
if __name__ == "__main__":

    try:
        asyncio.run(main_async())

    except Exception:
        traceback.print_exc()