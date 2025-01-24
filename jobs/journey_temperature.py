import requests
import random
from unittest.mock import MagicMock

TEMPERATURE_API_KEY = None


def simulate_temperature_api_call(lat, lon):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={TEMPERATURE_API_KEY}&units=metric"
        
        # Send a GET request to the API
        response = requests.get(url)
        if response.status_code!=200:
            raise Exception
        return response
    except:
        mock_response = MagicMock()
        mock_response.status_code = 200
        temp = 13 + random.uniform(-1, b=2)
        mock_response.json.return_value = {
                                            "coord": {
                                                "lon": lat,
                                                "lat": lon
                                            },
                                            "weather": [
                                                {
                                                    "id": 501,
                                                    "main": "Sunny",
                                                    "description": "moderate sunny",
                                                    "icon": "10d"
                                                }
                                            ],
                                            "base": "stations",
                                            "main": {
                                                "temperatureUnit": "celsius",
                                                "pressureUnit": "hectopascals",
                                                "temp": temp,
                                                "feels_like": temp+0.6,
                                                "temp_min": 9.6,
                                                "temp_max": 15.3,
                                                "pressure": 1013,
                                                "humidity": 35,
                                            },
                                            "visibility": 10000,
                                            "timezone": 0000,
                                            }
        return mock_response

if __name__=="__main__":
    print(simulate_temperature_api_call(1,2).json())