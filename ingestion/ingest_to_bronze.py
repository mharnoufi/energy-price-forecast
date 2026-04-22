import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

def get_supabase_data(table_name):
    url = os.environ.get("SUPABASE_URL").strip()
    key = os.environ.get("SUPABASE_KEY").strip()
    supabase = create_client(url, key)
    response = supabase.table(table_name).select("*").execute()
    return pd.DataFrame(response.data)

def upload_to_snowflake(df, table_name):
    conn_params = {
        "user": os.environ.get("SNOWFLAKE_USER").strip(),
        "password": os.environ.get("SNOWFLAKE_PASSWORD").strip(),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT").strip(),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE").strip(),
        "database": os.environ.get("SNOWFLAKE_DATABASE").strip(),
        "schema": "BRONZE",
        "role": "ACCOUNTADMIN"
    }

    conn = snowflake.connector.connect(**conn_params)
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE WAREHOUSE {conn_params['warehouse']}")
        cursor.execute(f"USE DATABASE {conn_params['database']}")
        cursor.execute(f"USE SCHEMA {conn_params['schema']}")

        write_pandas(
            conn, 
            df, 
            table_name.upper(), 
            auto_create_table=True
        )
        print(f"✅ {table_name} transférée dans {conn_params['database']}.{conn_params['schema']}")
    except Exception as e:
        print(f"Erreur lors de l'upload de {table_name} : {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("🚀 Démarrage du pipeline d'ingestion...")
    
    df_prices = get_supabase_data("raw_prices")
    upload_to_snowflake(df_prices, "RAW_PRICES")
    
    df_weather = get_supabase_data("raw_weather")
    upload_to_snowflake(df_weather, "RAW_WEATHER")
    
    print("🏁 Pipeline Bronze terminé avec succès !")