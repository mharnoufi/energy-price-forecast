import os
import requests
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv('weather_api')

CITIES = {
    "paris":    {"lat": 48.8566, "lon": 2.3522},
    "lyon":     {"lat": 45.7640, "lon": 4.8357},
    "marseille":{"lat": 43.2965, "lon": 5.3698},
    "bordeaux": {"lat": 44.8378, "lon": -0.5792},
    "lille":    {"lat": 50.6292, "lon": 3.0573},
}

def fetch_weather_archive(city_name, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2020-01-01",
        "end_date": "2024-12-31",
        "hourly": (
            "temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,"
            "cloud_cover,pressure_msl,wind_speed_100m,wind_direction_100m,"
            "shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance"
        ),
        "timezone": "UTC",
        "wind_speed_unit": "ms",
    }
    
    response = requests.get(URL, params=params)
    response.raise_for_status()
    data = response.json()
    hourly = data['hourly']

    return [
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