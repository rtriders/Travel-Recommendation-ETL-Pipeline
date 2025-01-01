import json
import boto3
from datetime import datetime
import requests
def lambda_handler(event, context):
    # Initialize variables
    Weather_list1 = []
    api_key = "408a589f7f2d505ff09be001d178006b"  # Replace with your actual API key
    cities = ["London", "New York", "Tokyo", "Mumbai", "Sydney", "Singapore", "Seoul", "Beijing", "Cape Town", "Moscow", "Paris", "Venice", "Berlin", "Ottawa", "Canberra", "Brasília", "Madrid", "Rome", "Bangkok", "Nairobi", "Buenos Aires", "Wellington", "Oslo", "Cairo", "Helsinki","Abu Dhabi"]
    
        
    # Base URL for the OpenWeather API
    base_url = "http://api.openweathermap.org/data/2.5/weather"

    # Function to fetch weather for a single city
    def get_weather(city, api_key):
        try:
            url = f"{base_url}?q={city}&appid={api_key}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                weather_info = {
                    "city": data["name"],
                    "temperature": round(data["main"]["temp"] - 273.15, 2),
                    "weather": data["weather"][0]["description"],
                    "humidity": data["main"]["humidity"],
                    "wind_speed": data["wind"]["speed"],
                    "min_temperature": round(data["main"]["temp_min"] - 273.15, 2),
                    "max_temperature": round(data["main"]["temp_max"] - 273.15, 2),
                    "sea_level": data["main"].get("sea_level", None)
                }
                return weather_info
            else:
                return {"city": city, "error": f"Error {response.status_code}: {response.text}"}
        except Exception as e:
            return {"city": city, "error": str(e)}

    # Fetch weather for all cities
    for city in cities:
        weather = get_weather(city, api_key)
        Weather_list1.append(weather)

    # Save weather data to S3
   
    s3 = boto3.client("s3")
    #filename = "weather_raw_" +str(datetime.now())+ ".json"
    
    s3.put_object(
        Bucket = "weather-reports-data",
        Key = "raw/weather_data.json",
        Body = json.dumps(Weather_list1)
    
    )

    

  

    

