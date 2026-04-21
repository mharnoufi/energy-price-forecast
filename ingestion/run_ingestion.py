import os
from dotenv import load_dotenv
from supabase import create_client
from entso_client import fetch_entsoe_data, parse_entsoe_data

load_dotenv()

def run_global_ingestion():
    token = os.environ.get("ENTSOE_TOKEN")
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not all([token, supabase_url, supabase_key]):
        print("Erreur : Variables d'environnement manquantes dans le .env")
        return

    supabase = create_client(supabase_url, supabase_key)
    full_history = []

    for year in range(2020, 2025):
        print(f"--- Récupération de l'année : {year} ---")
        
        # Format attendu par ENTSO-E : YYYYMMDD0000
        start = f"{year}01010000"
        end = f"{year + 1}01010000"
        
        try:
            # Appel du client
            raw_data = fetch_entsoe_data(token, start, end)
            # Parsing des données
            yearly_prices = parse_entsoe_data(raw_data)
            
            # ajout la liste globale
            full_history.extend(yearly_prices)
            print(f"Succès : {len(yearly_prices)} points récupérés.")
            
        except Exception as e:
            print(f"Erreur lors de l'année {year} : {e}")

    if full_history:
        # On utilise un dictionnaire pour garder uniquement la dernière occurrence de chaque timestamp
        unique_data = {item['timestamp_utc']: item for item in full_history}
        clean_history = list(unique_data.values())
        
        print(f"Après déduplication : {len(clean_history)} lignes uniques.")
        print(f"Envoi vers Supabase...")
        
        supabase.table("raw_prices").upsert(clean_history).execute()
        print("Ingestion terminée avec succès !")
    else:
        print("Aucune donnée à envoyer.")

if __name__ == "__main__":
    run_global_ingestion()