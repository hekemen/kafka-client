# Temperature App

 - Imagine that you have multiple temperature sensor send its value to your system
 - Ever sensor has id for exam: sensor-1, sensor-2
 - Ever temperature data is; timestamp|temperature

## Mission
 - Create Kafka producer and consumer to calculate average temperature of each sensor's data.
 - Image that, your producer will create records with this format; sensor-id|timestamp|temperature
 - Create Multiple Consumer and distribute to load.
 - Handle Consumer crashes. (Advanced)
 - Just print to console.