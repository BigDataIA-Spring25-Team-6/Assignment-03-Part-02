import requests
import boto3
import json
from datetime import datetime,timedelta
from dotenv import load_dotenv
import os
import time

load_dotenv()
api_token = os.getenv('NOAA_API_TOKEN')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Initialize S3 Client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

zip_codes = ["02128", "02130", "02186", "02766", "01602"]

# NOAA API endpoints for weather and station data
BASE_DATA_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
BASE_STATION_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/stations/"

# API headers
headers = {
    "token": api_token
}

# Global counter to track the number of API requests made in a day
daily_request_count = 0
start_of_day = datetime.now()

def upload_to_s3(data, s3_key):
    """
    Uploads data to S3 as a JSON file.
    """
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(data), ContentType='application/json')
        print(f"Uploaded data to s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload data to S3: {e}")

def rate_limit():
    """Enforce less than 5 requests per second and check the 10,000 requests per day limit."""
    global daily_request_count, start_of_day
    daily_request_count += 1
    # Reset the daily counter if a day has passed
    if datetime.now() - start_of_day > timedelta(days=1):
        daily_request_count = 1
        start_of_day = datetime.now()
    # Enforce maximum 10,000 requests per day
    if daily_request_count > 10000:
        raise Exception("Exceeded the daily API request limit of 10,000")
    # Pause for 0.5 seconds to allow less than 5 requests per second
    time.sleep(0.5)

def fetch_weather_data_for_zip(zip_code, start_date, end_date):
    """
    Retrieve weather data for a given zip code between start_date and end_date.
    This function splits the period into one‑year chunks and paginates results
    when more than 1,000 records are returned.
    """
    weather_data = []
    current_start = start_date

    while current_start < end_date:
        try:
            next_year = current_start.replace(year=current_start.year + 1)
        except ValueError:
            next_year = current_start + timedelta(days=365)
        # Limit interval to one year or less
        interval_end = min(next_year - timedelta(days=1), end_date)
        offset = 1
        limit = 1000  # API limit per request
        
        while True:
            params = [
                ('datasetid', 'GHCND'),
                ('locationid', f'ZIP:{zip_code}'),
                ('startdate', current_start.strftime("%Y-%m-%d")),
                ('enddate', interval_end.strftime("%Y-%m-%d")),
                ('datatypeid', 'TMAX'),
                ('datatypeid', 'TMIN'),
                ('limit', limit),
                ('offset', offset)
            ]
            # Retry API call up to 3 times if it fails
            retries = 3
            while retries > 0:
                rate_limit()  # Enforce rate limiting before sending the request
                response = requests.get(BASE_DATA_URL, headers=headers, params=params)
                if response.status_code == 200:
                    break
                else:
                    print(f"Error fetching weather data for zip {zip_code}: {response.status_code}. Retrying...")
                    time.sleep(0.5)
                    retries-=1
            if retries == 0:
                print(f"Error for zip {zip_code} from {current_start} to {interval_end}: {response.text}")
                break     
            data = response.json()
            if 'results' not in data:
                break
            weather_data.extend(data['results'])
            total_count = data.get("metadata", {}).get("resultset", {}).get("count", 0)
            if offset + limit > total_count:
                break
            offset += limit
        # Move to the next interval
        current_start = interval_end + timedelta(days=1)
    return weather_data

def fetch_station_data(station_id):
    """
    Retrieve station metadata for a given station using the NOAA API.
    """
    station_url = f"{BASE_STATION_URL}/{station_id}"
    retries = 3
    while retries > 0:
        rate_limit()  # Enforce rate limiting before the request
        response = requests.get(station_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching station data for {station_id}: {response.status_code}. Retrying...")
            time.sleep(0.5)
            retries-=1
    print(f"Failed to fetch station data for {station_id} after multiple attempts.")
    return None

def main():
    overall_start_date = datetime(2020, 1, 1)
    overall_end_date = datetime.now()

    # List to hold all the weather data and set to hold all the unique stations
    all_weather_data = []
    unique_station_ids = set()
    
    for zip_code in zip_codes:
        print(f"Processing zip code: {zip_code}")
        # Fetch weather data in segments handling pagination if results exceed 1,000 records
        weather_data = fetch_weather_data_for_zip(zip_code, overall_start_date, overall_end_date)

        # Adding the Zip code to all the fields
        for record in weather_data:
            record["zip_code"] = zip_code
        all_weather_data.extend(weather_data)

        # Collect unique station IDs from this zip code
        unique_station_ids.update({record["station"] for record in weather_data})
        
    # Save aggregated weather data for the zip code into the common JSON file
    weather_s3_key = f"weather_data.json"
    upload_to_s3(all_weather_data,weather_s3_key)
        
    # Fetch and upload metadata for each unique station as separate JSON files
    all_station_data = []
    for station_id in unique_station_ids:
        station_data = fetch_station_data(station_id)
        if station_data:
            all_station_data.append(station_data)
        
    # Save all station data to station file
    station_s3_key = f"station_data.json"
    upload_to_s3(all_station_data,station_s3_key)

if __name__ == "__main__":
    main()


