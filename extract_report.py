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

headers = {
    "Authorization": f"Bearer {api_key}"
}

timeout = httpx.Timeout(
    connect=10.0,
    read=300.0,
    write=30.0,
    pool=30.0
)

# STATUS
async def status():
    url_status = f"{url}/"

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url_status, headers=headers)
            response.raise_for_status()
            return response.json()

    except httpx.RequestError as e:
        print(f"An error occurred while requesting {e.request.url!r}: {e}")
        return None

#df = pd.DataFrame([status()])

#df.to_csv("status_report.csv", index=False)
##################################################################################

# recorrido de reportes
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
                return data["reports"]
            
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

# metric getters
def get_metric(items, key):
    return items.get(key) if isinstance(items, dict) else None

async def reports_all():
    count = 1000

    url_reports_all = f"{url}/reports"

    params_page = {
        "fields": "total_items",
        #"since_send_time": "2026-07-08T19:50:00+00:00",
    }

    semaphore = asyncio.Semaphore(2)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            print("Fetching total items...")
            response = await client.get(url_reports_all, headers=headers, params=params_page)
            response.raise_for_status()
            total_items = response.json().get("total_items", 0)
            total_pages = math.ceil(total_items / count)
            #total_items = 4*count
            
            print(f"Total items: {total_items}, Total pages: {total_pages}")
            # tasks = [
            #     fetch_page(client, url_reports_all, offset, count, semaphore)
            #     for offset in range(0, total_items, count)
            # ]

            # results = await asyncio.gather(*tasks, return_exceptions=True) 

            results = []

            for offset in range(0, total_items, count):
                print(f"Consultando offset {offset}")

                page = await fetch_page(
                    client,
                    url_reports_all,
                    offset,
                    count,
                    semaphore
                )

                results.append(page)

            df_mailchimp = build_dataframe(
                results,
                lambda r : {
                    "id": str(r.get("id")) if r.get("id") is not None else None, # id
                    "campaign_title": r.get("campaign_title"), # campaign_title
                    "list_id": r.get("list_id"), # list_id
                    "list_name": r.get("list_name"), # list_name
                    #"subject_line": r.get("subject_line"), # subject_line
                    "emails_sent": r.get("emails_sent"), # emails_sent
                    "abuse_reports": r.get("abuse_reports"), # abuse_reports
                    "unsubscribed": r.get("unsubscribed"), # unsubscribed
                    "send_time": r.get("send_time"), # send_time
                    "hard_bounces": get_metric(r.get("bounces"), "hard_bounces"), # hard_bounces
                    "soft_bounces": get_metric(r.get("bounces"), "soft_bounces"), # soft_bounces
                    "opens_total": get_metric(r.get("opens"), "opens_total"), # opens_total
                    "proxy_excluded_opens": get_metric(r.get("opens"), "proxy_excluded_opens"), # proxy_excluded_opens
                    "unique_opens": get_metric(r.get("opens"), "unique_opens"), # unique_opens
                    "proxy_excluded_unique_opens": get_metric(r.get("opens"), "proxy_excluded_unique_opens"), # proxy_excluded_unique_opens
                    "open_rate": get_metric(r.get("opens"), "open_rate"), # open_rate
                    "proxy_excluded_open_rate": get_metric(r.get("opens"), "proxy_excluded_open_rate"), # proxy_excluded_open_rate
                    "clicks_total": get_metric(r.get("clicks"), "clicks_total"), # clicks_total
                    "unique_clicks": get_metric(r.get("clicks"), "unique_clicks"), # unique_clicks
                    "unique_subscriber_clicks": get_metric(r.get("clicks"), "unique_subscriber_clicks"), # unique_subscriber_clicks
                    "click_rate": get_metric(r.get("clicks"), "click_rate"), # click_rate
                }
            )

            print(f"Total records fetched: {len(df_mailchimp)}")

            df_mailchimp["codigo_brief"] = df_mailchimp["campaign_title"].str.extract( # type: ignore
            r"(BRI-\d+(?:\s*-\s*[A-Za-z0-9]+)?)",
            expand=False
            ) # Extract numeric code from the 'Title' column

            df_mailchimp[["open_rate","click_rate","proxy_excluded_open_rate"]] = (
                df_mailchimp[["open_rate","click_rate","proxy_excluded_open_rate"]]
                .round(2)
            )

            df_mailchimp["send_time"] = (
                pd.to_datetime(df_mailchimp["send_time"], utc=True)
                .dt.tz_convert("America/Lima")
                .dt.tz_localize(None)
            )

            if df_mailchimp.empty:
                print("No hay datos para procesar.")
                return "SIN DATOS"
            
            df_mailchimp = df_mailchimp.drop_duplicates(
                subset=["id"],
                keep="first"
            )

            df_mailchimp.to_csv("mailchimp_report.csv", index=False)

            #print(df_mailchimp.dtypes)

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
                    for i in range(0, len(df_mailchimp), 100):
                        try:
                            print(f"Insertando filas {i} - {i+100}")

                            df_mailchimp.iloc[i:i+100].to_sql(
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
        asyncio.run(reports_all())
        print("Proceso completado.")
    except Exception as e:
        traceback.print_exc()