Build docker image with:
```
docker build . -f docker/Dockerfile -t tdm/iotawatt_publisher
```

Config file example:

```
[IOTAWATT_publisher]
mqtt_host = mosquitto
mqtt_port = 1883
logging_level = 0
influxdb_host = influxdb
influxdb_port = 8086
```
