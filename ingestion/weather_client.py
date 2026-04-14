import os
import requests
import pandas as pd # Recommandé pour manipuler ce volume de données
from dotenv import load_dotenv

load_dotenv()

url = os.getenv('weather_api') # archive-api.open-meteo.com/v1/archive

CITIES ={
"paris":    {"lat": 48.8566, "lon": 2.3522, "weight": 0.35},
"lyon":     {"lat": 45.7640, "lon": 4.8357, "weight": 0.15},
"marseille":{"lat": 43.2965, "lon": 5.3698, "weight": 0.15},
"bordeaux": {"lat": 44.8378, "lon": -0.5792, "weight": 0.15},
"lille":    {"lat": 50.6292, "lon": 3.0573, "weight": 0.20},
}

all_cities_data = []

for city_name, coords in CITIES.items():
    print(f"Récupération des données pour : {city_name}...")

    params ={
    "latitude": coords["lat"],
    "longitude": coords["lon"],
    "start_date": "2020-01-01",
    "end_date": "2024-12-31",
    "hourly": (
            "temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,"
            "cloud_cover,pressure_msl,wind_speed_100m,wind_direction_100m,"
            "shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance"
        ),    "timezone": "Europe/Paris",
    "wind_speed_unit": "ms",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    hourly = data['hourly']

    # On utilise zip() sur tous les paramètres souhaités
    # On rajoute city_name pour identifier la ligne en base de données
    city_data = [
        {
            "timestamp_utc": t,
            "city": city_name,
            "temp": temp,
            "temp_app": app_temp,
            "hum": hum,
            "precip": prec,
            "clouds": cloud,
            "press": press,
            "w_speed_100": ws100,
            "w_dir_100": wd100,
            "rad_short": rad_s,
            "rad_dir": rad_dr,
            "rad_diff": rad_df,
            "dni": dni
        }
        for t, temp, app_temp, hum, prec, cloud, press, ws100, wd100, rad_s, rad_dr, rad_df, dni in zip(
            hourly['time'],
            hourly['temperature_2m'],
            hourly['apparent_temperature'],
            hourly['relative_humidity_2m'],
            hourly['precipitation'],
            hourly['cloud_cover'],
            hourly['pressure_msl'],
            hourly['wind_speed_100m'],
            hourly['wind_direction_100m'],
            hourly['shortwave_radiation'],
            hourly['direct_radiation'],
            hourly['diffuse_radiation'],
            hourly['direct_normal_irradiance']
        )
    ]
    all_cities_data.extend(city_data)

    print(f"Nombre total de lignes : {len(all_cities_data)}")
    print(f"Exemple (Paris) : {all_cities_data[0]}")

    print(f"Total lignes : {len(all_cities_data)}")