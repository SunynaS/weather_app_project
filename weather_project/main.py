import logging
from datetime import datetime

import geocoder
import requests
from pymongo import MongoClient

logging.basicConfig(filename='app.log', level=logging.INFO)


# Hitting GET Request
def hit_get_request(url: str, headers: dict = dict(), query_params=dict()) -> dict:
    username = 'others_salman'
    password = 'Hkz6dWB20U'
    logging.info(f"Initiating Request {url}, {headers}, {query_params}, auth-> {username}, {password}")
    resp = requests.get(url, auth=(username, password))
    logging.info(
        f"Response {url}, {headers}, {query_params}, auth-> {username}, {password}, Response -> {resp.status_code}, {resp.text}")
    if resp.status_code not in range(200, 300):
        raise ValueError(f"Getting error for request -> {url}, {headers}, {query_params}, Response -> {resp.text}")
    return resp.json()


# Giving connection object of that collection of mongo DB
def get_mongodb_connection():
    client = MongoClient('localhost', 27017)
    return client.weather_database.weather_details


def fetch_current_weather_data() -> dict:
    logging.info("Fetching weather data")
    current_time = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
    temps_unit = "t_2m:C"
    output_format = "json"
    current_location_lat_long = ','.join(map(str, geocoder.ip('me').latlng))
    url = f"https://api.meteomatics.com/{current_time}/{temps_unit}/{current_location_lat_long}/{output_format}"
    logging.info(
        f"Calculated Parameters {current_time}, {temps_unit}, {output_format}, {current_location_lat_long}, {url}")
    data = hit_get_request(url)
    temperature_data = data.get("data")[0].get("coordinates")[0]
    result = {'user': data.get("user"), 'dateGenerated': data.get("dateGenerated"), 'data': temperature_data}
    return result


def main():
    logging.info(f"Initiating Procedure @{datetime.now()}")
    data = fetch_current_weather_data()
    logging.info("Fetching Data Done")
    conn = get_mongodb_connection()
    logging.info("MongoDB Connection is connected successfully!!")
    conn.insert_one(data)
    logging.info("Data inserted successfully!!")
    logging.info(f"End Procedure @{datetime.now()}")


if __name__ == '__main__':
    main()
