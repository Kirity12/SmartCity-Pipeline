import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
import uuid
import json
from car_snapshot import extract_frame_at_timestamp
from journey_temperature import simulate_temperature_api_call



LONDON_COORDINATES = {
    "latitude": 51.5074,
    "longitude": -0.1278
}

BIRMINGHAM_COORDINATES = {
    "latitude": 52.4862,
    "longitude": -1.8904
}

LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES['latitude'])/100
import os
from confluent_kafka import SerializingProducer
import simplejson as json

LONDON_COORDINATES = {
    "latitude": 51.5074,
    "longitude": -0.1278,
    "altitude": 50 + random.uniform(0, 1000) 
}

BIRMINGHAM_COORDINATES = {
    "latitude": 52.4862,
    "longitude": -1.8904,
    "altitude": 50 + random.uniform(0, 1000)
}

# Calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES['latitude'])/100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES['longitude'])/100

# environment variables for config
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = current_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_vehicle_location():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT 
    start_location['altitude'] = 50 + random.uniform(0, 1000)  # Altitude in meters (simulating slight change)


    # add randomness to increment
    start_location['latitude']+=random.uniform(-0.0005, 0.0005)
    start_location['longitude']+=random.uniform(-0.0005, 0.0005)
    
    return start_location

def get_next_time():
    global current_time
    current_time += timedelta(seconds=random.randint(a=30, b=60))
    return current_time

def get_temperature_data(location):
    # Base URL for the OpenWeatherMap API
    lat, lon = location
    response = simulate_temperature_api_call(lat, lon)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        return data
    else:
        # If there was an error, print the error message
        print(f"Error: {response.status_code}, Could not retrieve data")
        return None


def generate_vehicle_data(device):
    global start_time
    current_time = get_next_time()
    time_delta = (current_time - start_time).total_seconds()
    fuel_level = 47 - (time_delta * 0.05) - random.uniform(a=0, b=0.03)
    engine_temperature = random.uniform(85, 110)  # Engine temperature in Celsius
    battery_voltage = random.uniform(12.0, 14.0)  # Battery voltage in volts
    tire_pressure = random.uniform(30, 35)  # Tire pressure in psi
    rpm = random.uniform(1000, 5000)  # Engine RPM
    throttle_position = random.uniform(0, 100)  # Throttle position in percentage
    mileage = time_delta * random.uniform(0.02, 0.1)  # Simulate mileage in km
    ambient_temperature = random.uniform(10, 35)
    return {
        'id': uuid.uuid4(),
        'deviceId': device,
        'timestamp': current_time.isoformat(),
        'fuel': fuel_level,
        "engineTemperature": round(engine_temperature, 2),
        "batteryVoltage": round(battery_voltage, 2),
        "tirePressure": round(tire_pressure, 2),
        "engineRpm": round(rpm, 0),
        "throttlePosition": round(throttle_position, 2),
        "mileage": round(mileage, 2),
        "ambientTemperature": round(ambient_temperature, 2),
        'make': 'Mazda',
        'model': 'CX5',
        'year': 2024,
        'fuelType': 'petrol'
    }

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    location = get_vehicle_location()
    return {
        'id': uuid.uuid4(),
        'devideId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(a=0, b=40),
        "altitude": location['altitude'],
        'direction': 'ne',
        'accuracy': random.uniform(3, 10),
        'location': (location['latitude'], location['longitude']),
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    global start_time
    time_delta = datetime.fromisoformat(timestamp) - start_time
    time_in_seconds = time_delta.total_seconds()
    return {
        'id': uuid.uuid4(),
        'devideId': device_id,
        'timestamp': timestamp,
        'location': location,
        'cameraId': camera_id,
        'snapshot': extract_frame_at_timestamp(time_in_seconds)
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'devideId': device_id,
        'timestamp': timestamp,
        'location': location,
        'weatherData': get_temperature_data(location)
    }

def generate_emergency_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'devideId': device_id,
        'timestamp': timestamp,
        'location': location,
        'incidentId': uuid.uuid4(),
        'incidentType': 'None',
        'incidentStatus': 'Resolved',
        'incidentDescription': 'No issue'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, mssg):
    if err is not None:
        print(f'Message delivery error: {err}')
    else:
        print(f'Message delivered to {mssg.topic()} [{mssg.partition()}]')


def produce_data_to_kafka(producer: SerializingProducer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

def simulate_journey(producer, device):
    while True:
        vehicle_data = generate_vehicle_data(device)
        gps_data = generate_gps_data(device, vehicle_data['timestamp'])
        traffic_data = generate_traffic_camera_data(device, vehicle_data['timestamp'], gps_data['location'], 0)
        weather_data = generate_weather_data(device, vehicle_data['timestamp'], gps_data['location'])
        emergency_data = generate_emergency_data(device, vehicle_data['timestamp'], gps_data['location'])
        
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

        break


if __name__=="__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Producer-01')
    
    except KeyboardInterrupt:
        print("Simulation Ended by User")
    except Exception as e:
        print(f"Unexpected Error: {e}")
