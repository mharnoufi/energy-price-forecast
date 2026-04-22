import os
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window
from dotenv import load_dotenv

load_dotenv()

def get_session():
    connection_parameters = {
        "user": os.environ.get("SNOWFLAKE_USER").strip(),
        "password": os.environ.get("SNOWFLAKE_PASSWORD").strip(),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT").strip(),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE").strip(),
        "database": os.environ.get("SNOWFLAKE_DATABASE").strip(),
        "schema": "BRONZE",
        "role": "ACCOUNTADMIN"
    }
    return Session.builder.configs(connection_parameters).create()

def clean_prices(session):
    df_prices = session.table("BRONZE.RAW_PRICES")
    df_prices = df_prices.with_column('"timestamp_utc"', F.to_timestamp(F.col('"timestamp_utc"')))
    return df_prices

def clean_weather(session):
    df_weather = session.table("BRONZE.RAW_WEATHER")
    df_weather = df_weather.with_column('"timestamp_utc"', F.to_timestamp(F.col('"timestamp_utc"')))

    villes = [row[0] for row in df_weather.select('"city"').distinct().collect()]
    
    df_pivoted = df_weather.pivot('"city"', villes).avg('"temp"')
    
    for v in villes:
        old_col_name = F.col(f"'{v}'") 
        new_col_name = f"TEMP_{v.upper()}"
        
        df_pivoted = df_pivoted.with_column_renamed(old_col_name, new_col_name)
        
    return df_pivoted

def create_silver_table(session, df_prices, df_weather):
    master_df = df_prices.join(df_weather, '"timestamp_utc"', "left")
    
    window_spec = Window.order_by('"timestamp_utc"').rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
    
    master_df = master_df.with_column(
        "PRICE_CLEAN", 
        F.last_value(F.col('"price_eur_mwh"'), ignore_nulls=True).over(window_spec)
    )
    
    master_df = master_df.with_column("HOUR", F.hour(F.col('"timestamp_utc"'))) \
                         .with_column("DAY_OF_WEEK", F.dayofweek(F.col('"timestamp_utc"'))) \
                         .with_column("MONTH", F.month(F.col('"timestamp_utc"'))) \
                         .with_column("IS_WEEKEND", F.when(F.dayofweek(F.col('"timestamp_utc"')).isin(6, 0), 1).otherwise(0))
    
    session.sql("CREATE SCHEMA IF NOT EXISTS SILVER").collect()
    master_df.write.mode("overwrite").save_as_table("SILVER.CLEANED_ENERGY_DATA")

if __name__ == "__main__":
    session = get_session()
    try:
        print("Nettoyage des prix...")
        df_p = clean_prices(session)
        
        print("Nettoyage et pivot de la météo...")
        df_w = clean_weather(session)
        
        print("Fusion et création de la table SILVER...")
        create_silver_table(session, df_p, df_w)
        
        print("table SILVER ok ")
    except Exception as e:
        print(f"Erreur lors du processing : {e}")
    finally:
        session.close()