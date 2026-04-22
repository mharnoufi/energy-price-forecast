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
    
    order_col = "city" if table_name == "raw_weather" else "timestamp_utc"
    
    all_data = []
    page_size = 1000
    start = 0
    
    print(f"📥 Extraction de {table_name} par paquets de {page_size}...")

    while True:
        # On demande la "page" actuelle
        stop = start + page_size - 1
        response = (
            supabase.table(table_name)
            .select("*")
            .order(order_col)
            .range(start, stop)
            .execute()
        )
        
        batch = response.data
        if not batch: # Si plus de données, on arrête
            break
            
        all_data.extend(batch)
        print(f"   -> {len(all_data)} lignes récupérées...")
        
        # On passe à la page suivante
        start += page_size
        
        # Sécurité pour ne pas boucler à l'infini si ton dataset est géant
        if start >= 100000: 
            break

    df = pd.DataFrame(all_data)
    
    if table_name == "raw_weather" and not df.empty:
        print(f"✅ Terminé ! Villes trouvées : {df['city'].unique()}")
        print(df['city'].value_counts())
        
    return df
    url = os.environ.get("SUPABASE_URL").strip()
    key = os.environ.get("SUPABASE_KEY").strip()
    supabase = create_client(url, key)
    
    order_col = "city" if table_name == "raw_weather" else "timestamp_utc"
    
    # On monte à 60 000 pour être sûr de dépasser Bordeaux
    # Si c'est encore trop juste, on montera plus haut
    limit = 60000 
    
    print(f"📥 Récupération de {table_name} (0 à {limit})...")
    
    response = (
        supabase.table(table_name)
        .select("*")
        .order(order_col)
        .range(0, limit)
        .execute()
    )
    
    df = pd.DataFrame(response.data)
    
    if table_name == "raw_weather":
        villes = df['city'].unique()
        print(f"📍 Villes récupérées : {villes}")
        print(f"📊 Nombre de lignes par ville :")
        print(df['city'].value_counts())
        
    return df
    url = os.environ.get("SUPABASE_URL").strip()
    key = os.environ.get("SUPABASE_KEY").strip()
    supabase = create_client(url, key)
    
    # la colonne de tri selon la table
    order_col = "city" if table_name == "raw_weather" else "timestamp_utc"
    
    print(f"📥 Récupération de {table_name} (tri par {order_col})...")
    
    response = (
        supabase.table(table_name)
        .select("*")
        .order(order_col)
        .range(0, 10000)
        .execute()
    )
    
    df = pd.DataFrame(response.data)
    
    if table_name == "raw_weather":
        print(f"Villes récupérées : {df['city'].unique()}")
        
    return df

    url = os.environ.get("SUPABASE_URL").strip()
    key = os.environ.get("SUPABASE_KEY").strip()
    supabase = create_client(url, key)
    response = supabase.table(table_name).select("*").order("city").range(0,10000).execute()
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
        print(f"{table_name} transférée dans {conn_params['database']}.{conn_params['schema']}")
    except Exception as e:
        print(f"Erreur lors de l'upload de {table_name} : {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("Démarrage du pipeline d'ingestion...")
    
    df_prices = get_supabase_data("raw_prices")
    upload_to_snowflake(df_prices, "RAW_PRICES")
    
    df_weather = get_supabase_data("raw_weather")
    upload_to_snowflake(df_weather, "RAW_WEATHER")
    
    print("Pipeline Bronze terminé avec succès !")