import os
import time
from dotenv import load_dotenv, find_dotenv
from supabase import create_client, Client
from weather_client import CITIES, fetch_weather_archive

load_dotenv(find_dotenv())

def get_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not key:
        raise ValueError("SUPABASE_KEY manquante.")
    return create_client(url, key)

def ingest_data(supabase, table_name, data):
    BATCH_SIZE = 2000
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i : i + BATCH_SIZE]
        supabase.table(table_name).upsert(batch).execute()

def run_pipeline():
    supabase = get_supabase_client()
    
    for city, coords in CITIES.items():
        try:
            print(f"--- Extraction : {city} ---")
            weather_data = fetch_weather_archive(city, coords["lat"], coords["lon"])
            
            print(f"--- Ingestion : {city} ---")
            ingest_data(supabase, "raw_weather", weather_data)
            
            print(f"✅ {city} terminé.")
            time.sleep(5)
            
        except Exception as e:
            print(f"❌ Erreur sur {city} : {e}")
            time.sleep(10)

if __name__ == "__main__":
    run_pipeline()