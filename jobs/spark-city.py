from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.dataframe import DataFrame
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0, org.apache.hadoop:hadoop-aws:3.3.1, com.amazonaws:aws-java-sdk:1.11.469 pyspark-shell'


def log_batch(df, epoch_id):
    print(f"Processing batch with epoch_id: {epoch_id}")

AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''


def main():

    spark = SparkSession.builder \
        .appName("JourneySparkStreamingService") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
                org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    # Adjust log level
    spark.sparkContext.setLogLevel('WARN')

    def read_kafka_topic(topic, schema):
        return spark.readStream\
                .format('kafka')\
                .option('kafka.bootstrap.servers', 'broker:29092')\
                .option('subscribe', topic)\
                .option('startingOffsets', 'earliest')\
                .load()\
                .selectExpr('CAST(value as STRING)')\
                .select(from_json(col('value'), schema).alias('data'))\
                .select('data.*')\
                .withWatermark('timestamp', delayThreshold='2 minutes')
    
    def streamWriter(input: DataFrame, checkpoint, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpoint)
                .option('path', output)
                .outputMode('append')
                .start())

# .option('startingOffsets', 'earliest')\

    vehicle_schema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('fuel', FloatType(), True),
        StructField('engineTemperature', FloatType(), True),
        StructField('speed', FloatType(), True),
        StructField('batteryVoltage', FloatType(), True),
        StructField('tirePressure', FloatType(), True),
        StructField('throttlePosition', FloatType(), True),
        StructField('mileage', FloatType(), True),
        StructField('ambientTemperature', FloatType(), True),
        StructField('engineRpm', FloatType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('fuelType', StringType(), True),
    ])

    gps_schema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('altitude', FloatType(), True),
        StructField('direction', StringType(), True),
        StructField('accuracy', StringType(), True),
        StructField('location', StringType(), True),
        StructField('vehicleType', StringType(), True),
    ])

    traffic_schema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('cameraId', IntegerType(), True),
        StructField('snapshot', StringType(), True),
    ])

    weather_schema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('weatherData', StringType(), True),
    ])

    emergency_schema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('incidentId', StringType(), True),
        StructField('incidentType', StringType(), True),
        StructField('incidentStatus', StringType(), True),
        StructField('incidentDescription', StringType(), True),
        
    ])

    vehicle_df = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    gps_df = read_kafka_topic('gps_data', gps_schema).alias('gps')
    traffic_df = read_kafka_topic('traffic_data', traffic_schema).alias('traffic')
    weather_df = read_kafka_topic('weather_data', weather_schema).alias('weather')
    emergency_df = read_kafka_topic('emergency_data', emergency_schema).alias('emergency')

    query1 = streamWriter(vehicle_df, 's3a://vehicle-streaming-data/checkpoints/vehicle_data', 's3a://vehicle-streaming-data/data/vehicle_data')
    query2 = streamWriter(gps_df, 's3a://vehicle-streaming-data/checkpoints/gps_data', 's3a://vehicle-streaming-data/data/gps_data')
    query3 = streamWriter(traffic_df, 's3a://vehicle-streaming-data/checkpoints/traffic_data', 's3a://vehicle-streaming-data/data/traffic_data')
    query4 = streamWriter(weather_df, 's3a://vehicle-streaming-data/checkpoints/weather_data', 's3a://vehicle-streaming-data/data/weather_data')
    query5 = streamWriter(emergency_df, 's3a://vehicle-streaming-data/checkpoints/emergency_data', 's3a://vehicle-streaming-data/data/emergency_data')

    query5.awaitTermination()



if __name__=="__main__":
    main()